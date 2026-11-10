package dev.forgetalker;

import dev.forgetalker.bridge.ClusterBridge;
import dev.forgetalker.bridge.ClusterMetrics;
import dev.forgetalker.bridge.ClusterTransport;
import dev.forgetalker.bridge.StandaloneTransport;
import dev.forgetalker.bridge.message.MutationMessages.BlockMutation;
import dev.forgetalker.bridge.message.MutationMessages.EntitySpawn;
import dev.forgetalker.bridge.net.MultiPaperTcpTransport;
import dev.forgetalker.cluster.ChunkCoordinate;
import dev.forgetalker.cluster.ChunkOwnershipGuard;
import dev.forgetalker.cluster.ChunkOwnershipRegistry;
import dev.forgetalker.cluster.ClusterNodeId;
import dev.forgetalker.cluster.MutationVerdict;
import dev.forgetalker.cluster.NodeIdProvider;
import dev.forgetalker.config.ForgeTalkerConfig;
import dev.forgetalker.config.RuntimeSettings;
import dev.forgetalker.dispatch.PeerMessageDispatcher;
import dev.forgetalker.util.MixinProbe;
import net.minecraft.core.BlockPos;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.state.BlockState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Composition root and static entry point for everything that needs the cluster at runtime.
 *
 * <h2>Operational purpose</h2>
 * Constructs the object graph (settings -> node id -> registry -> transport -> bridge -> guard ->
 * dispatcher) once per server session and exposes <em>static</em> helpers that mixins and Forge event
 * handlers can call without holding a reference. Mixins are merged into Minecraft classes and cannot
 * receive constructor arguments, so a static accessor is the only practical bridge.
 *
 * <h2>Null-safety contract (critical for agents)</h2>
 * Every static helper in this class is safe to call <em>before</em> {@link #bootstrap(MinecraftServer)}
 * and <em>after</em> {@link #shutdown(String)}: when no runtime exists they return permissive values
 * ({@code ALLOW} verdicts, no-op relays). This matters because {@code Level.setBlock} runs during
 * world load, before {@code ServerStartedEvent}, and during shutdown, after the cluster is gone. A
 * guard that threw or denied here would corrupt world loading.
 *
 * <h2>Thread safety</h2>
 * The instance is published through a {@code volatile} field. All collaborators are themselves
 * thread-safe: the registry uses a concurrent map, the transport is asynchronous, the guard reads
 * volatile suppliers, and the dispatcher hops to the server thread for anything world related.
 * The static helpers may therefore be called from the Netty event loop (payload relay), the server
 * thread (mixins, events) and the command thread.
 */
public final class ForgeTalkerRuntime {

    private static final Logger LOGGER = LogManager.getLogger("ForgeTalker");

    /** Bootstrapped runtime, or {@code null} when the server is not running. */
    private static volatile ForgeTalkerRuntime instance;

    /** Current server instance, used by the dispatcher to hop onto the server thread. */
    private static volatile MinecraftServer currentServer;

    private final RuntimeSettings settings;
    private final ClusterNodeId nodeId;
    private final ClusterMetrics metrics;
    private final ChunkOwnershipRegistry registry;
    private final ChunkOwnershipGuard guard;
    private final ClusterBridge bridge;
    private final PeerMessageDispatcher dispatcher;

    /**
     * @param settings   settings snapshot used to build the graph, never {@code null}
     * @param nodeId     this node's stable id, never {@code null}
     * @param metrics    counters, never {@code null}
     * @param registry   ownership mirror, never {@code null}
     * @param guard      ownership guard, never {@code null}
     * @param bridge     cluster facade, never {@code null}
     * @param dispatcher inbound frame dispatcher, never {@code null}
     */
    private ForgeTalkerRuntime(RuntimeSettings settings,
                              ClusterNodeId nodeId,
                              ClusterMetrics metrics,
                              ChunkOwnershipRegistry registry,
                              ChunkOwnershipGuard guard,
                              ClusterBridge bridge,
                              PeerMessageDispatcher dispatcher) {
        this.settings = settings;
        this.nodeId = nodeId;
        this.metrics = metrics;
        this.registry = registry;
        this.guard = guard;
        this.bridge = bridge;
        this.dispatcher = dispatcher;
    }

    /**
     * Builds the runtime graph and starts the transport.
     *
     * <h2>Threading</h2>
     * Called from {@code ServerStartedEvent} on the server thread. The transport only schedules
     * asynchronous work, so this method returns quickly.
     *
     * @param server running server, never {@code null}
     */
    public static void bootstrap(MinecraftServer server) {
        Objects.requireNonNull(server, "server");
        if (instance != null) {
            LOGGER.warn("Forge Talker runtime already bootstrapped; ignoring duplicate start");
            return;
        }
        currentServer = server;
        RuntimeSettings settings = ForgeTalkerConfig.active();
        ClusterNodeId nodeId = NodeIdProvider.resolve(settings);
        ClusterMetrics metrics = new ClusterMetrics();
        ChunkOwnershipRegistry registry = new ChunkOwnershipRegistry();
        ClusterTransport transport = settings.clusterEnabled()
                ? new MultiPaperTcpTransport(settings, nodeId, metrics)
                : new StandaloneTransport();
        ClusterBridge bridge = new ClusterBridge(settings, nodeId, registry, metrics, transport);
        ChunkOwnershipGuard guard = new ChunkOwnershipGuard(registry, () -> nodeId,
                bridge::isClusterActive, () -> ForgeTalkerConfig.active().assumeOwnershipWhenUnknown());
        PeerMessageDispatcher dispatcher = new PeerMessageDispatcher(() -> currentServer, bridge, guard,
                metrics, ForgeTalkerConfig::active);
        transport.addListener(dispatcher);

        ForgeTalkerRuntime runtime = new ForgeTalkerRuntime(settings, nodeId, metrics, registry, guard,
                bridge, dispatcher);
        instance = runtime;
        bridge.start();
        LOGGER.info("Forge Talker bootstrapped node '{}' (clusterEnabled={}, master={}:{}, guards block={} entity={})",
                nodeId, settings.clusterEnabled(), settings.masterHost(), settings.masterPort(),
                settings.enforceBlockMutationGuard(), settings.enforceEntityGuard());
        // Runtime proof that the bytecode guards were actually merged into the Minecraft classes.
        // A missing marker means the refmap did not apply and the cluster is unprotected.
        LOGGER.info("Forge Talker mixins applied: {}", MixinProbe.applied());
        if (settings.clusterEnabled() && settings.clusterSecret().isBlank()) {
            LOGGER.warn("clusterEnabled=true but clusterSecret is empty; the master will reject anonymous nodes");
        }
    }

    /**
     * Stops the transport and drops the runtime.
     *
     * @param reason reason logged and sent to the cluster, never {@code null}
     */
    public static void shutdown(String reason) {
        ForgeTalkerRuntime runtime = instance;
        instance = null;
        if (runtime != null) {
            runtime.bridge.stop(reason);
            runtime.dispatcher.shutdown();
            LOGGER.info("Forge Talker node '{}' shut down ({})", runtime.nodeId, reason);
        }
        currentServer = null;
    }

    /**
     * @return the active runtime, or {@code null} when the server is not running
     */
    public static ForgeTalkerRuntime get() {
        return instance;
    }

    /**
     * @return {@code true} when a runtime exists and the cluster session is authoritative
     */
    public static boolean isClusterActive() {
        ForgeTalkerRuntime runtime = instance;
        return runtime != null && runtime.bridge.isClusterActive();
    }

    /**
     * @return the settings of the active runtime, or standalone defaults when none is active
     */
    public static RuntimeSettings settings() {
        ForgeTalkerRuntime runtime = instance;
        return runtime == null ? ForgeTalkerConfig.active() : runtime.settings;
    }

    /**
     * @return the active runtime's node id, or {@code null} when none is active
     */
    public ClusterNodeId nodeId() {
        return nodeId;
    }

    /**
     * @return the metric counters, never {@code null}
     */
    public ClusterMetrics metrics() {
        return metrics;
    }

    /**
     * @return the ownership mirror, never {@code null}
     */
    public ChunkOwnershipRegistry registry() {
        return registry;
    }

    /**
     * @return the ownership guard, never {@code null}
     */
    public ChunkOwnershipGuard guard() {
        return guard;
    }

    /**
     * @return the cluster facade, never {@code null}
     */
    public ClusterBridge bridge() {
        return bridge;
    }

    /**
     * @return the inbound frame dispatcher, never {@code null}
     */
    public PeerMessageDispatcher dispatcher() {
        return dispatcher;
    }

    /**
     * @return the settings this runtime was built with, never {@code null}
     */
    public RuntimeSettings effectiveSettings() {
        return settings;
    }

    /**
     * Evaluates a block mutation for the mixin guard.
     *
     * @param level level being mutated, may be {@code null} (defensive)
     * @param pos   mutated position, may be {@code null} (defensive)
     * @return the verdict; {@code ALLOW} when no runtime, no guard or the guard is disabled
     * @implNote Never throws: an exception here would propagate into {@code Level.setBlock} and crash
     *         the tick loop. Failures are logged and treated as ALLOW, preferring availability over
     *         split-brain protection for a single frame.
     */
    public static MutationVerdict evaluateBlockMutation(Level level, BlockPos pos) {
        ForgeTalkerRuntime runtime = instance;
        if (runtime == null || level == null || pos == null
                || !runtime.effectiveSettings().enforceBlockMutationGuard()) {
            return MutationVerdict.allow("guard inactive");
        }
        try {
            return runtime.guard.evaluateBlockMutation(level, pos);
        } catch (RuntimeException e) {
            LOGGER.warn("Ownership guard failed; allowing mutation at {}: {}", pos, e.toString());
            return MutationVerdict.allow("guard failure");
        }
    }

    /**
     * Evaluates an entity spawn for the mixin guard.
     *
     * @param entity entity about to be added, may be {@code null}
     * @return the verdict; {@code ALLOW} when no runtime, no guard or the guard is disabled
     * @implNote Never throws, for the same reason as {@link #evaluateBlockMutation(Level, BlockPos)}.
     */
    public static MutationVerdict evaluateEntityMutation(Entity entity) {
        ForgeTalkerRuntime runtime = instance;
        if (runtime == null || entity == null
                || !runtime.effectiveSettings().enforceEntityGuard()) {
            return MutationVerdict.allow("guard inactive");
        }
        try {
            return runtime.guard.evaluateEntityMutation(entity);
        } catch (RuntimeException e) {
            LOGGER.warn("Entity guard failed; allowing spawn of {}: {}", entity.getType(), e.toString());
            return MutationVerdict.allow("guard failure");
        }
    }

    /**
     * Forwards a blocked block mutation to the node that owns the chunk.
     *
     * @param level level that was mutated, never {@code null}
     * @param pos   mutated position, never {@code null}
     * @param state requested block state, never {@code null}
     * @param flags vanilla update flags
     * @implNote Called from the server thread inside {@code Level.setBlock}. Building the frame is
     *         allocation-light; the actual network write is asynchronous.
     */
    public static void relayBlockMutation(ServerLevel level, BlockPos pos, BlockState state, int flags) {
        ForgeTalkerRuntime runtime = instance;
        if (runtime == null) {
            return;
        }
        try {
            ClusterNodeId nodeId = runtime.nodeId;
            ChunkCoordinate chunk = ChunkCoordinate.fromBlock(
                    level.dimension().location().toString(), pos.getX(), pos.getZ());
            runtime.bridge.relayBlockMutation(new BlockMutation(chunk, pos.getX(), pos.getY(),
                    pos.getZ(), Block.getId(state), flags, nodeId.value()));
            runtime.metrics.recordBlockedMutation();
            if (runtime.effectiveSettings().debugLogging()) {
                LOGGER.debug("Relayed block mutation {} at {} to the owning node", state, pos);
            }
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to relay block mutation at {}: {}", pos, e.toString());
        }
    }

    /**
     * Forwards a blocked entity spawn to the node that owns the entity's chunk.
     *
     * @param level  level the entity was going to join, never {@code null}
     * @param entity entity that was blocked, never {@code null}
     * @implNote Serialises the entity NBT so equipment, attributes and mod data survive the transfer.
     *         Callers run on the server thread, which is required for {@code saveWithoutId}.
     */
    public static void relayEntitySpawn(ServerLevel level, Entity entity) {
        ForgeTalkerRuntime runtime = instance;
        if (runtime == null) {
            return;
        }
        try {
            ChunkCoordinate chunk = ChunkCoordinate.fromBlock(
                    level.dimension().location().toString(), entity.blockPosition().getX(),
                    entity.blockPosition().getZ());
            byte[] nbt = null;
            try {
                CompoundTag tag = entity.saveWithoutId(new CompoundTag());
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                try (DataOutputStream out = new DataOutputStream(bytes)) {
                    NbtIo.write(tag, out);
                }
                nbt = bytes.toByteArray();
            } catch (IOException | RuntimeException e) {
                LOGGER.debug("Could not serialise entity NBT for relay: {}", e.toString());
            }
            String typeId = BuiltInRegistries.ENTITY_TYPE.getKey(entity.getType()).toString();
            runtime.bridge.relayEntitySpawn(new EntitySpawn(chunk, entity.getUUID(), typeId,
                    entity.getX(), entity.getY(), entity.getZ(), entity.getYRot(), entity.getXRot(),
                    nbt, runtime.nodeId.value()));
            runtime.metrics.recordBlockedMutation();
            if (runtime.effectiveSettings().debugLogging()) {
                LOGGER.debug("Relayed entity spawn {} ({}) to the owning node",
                        entity.getUUID(), typeId);
            }
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to relay entity spawn {}: {}", entity.getUUID(), e.toString());
        }
    }

    /**
     * Drains deferred mutations. Intended to be called once per server tick.
     */
    public static void drainDeferred() {
        ForgeTalkerRuntime runtime = instance;
        if (runtime != null) {
            runtime.dispatcher.drainDeferred();
        }
    }

    /**
     * Builds the multi-line status report used by {@code /forgetalker status}.
     *
     * @return an immutable list of human readable lines, never {@code null}
     */
    public static List<String> statusLines() {
        ForgeTalkerRuntime runtime = instance;
        List<String> lines = new ArrayList<>();
        if (runtime == null) {
            lines.add("Forge Talker runtime is not active (server starting or stopping).");
            lines.add("Applied mixins: " + MixinProbe.applied());
            return lines;
        }
        lines.add("Node: " + runtime.nodeId + " (cluster active: " + runtime.bridge.isClusterActive() + ")");
        lines.add("Transport: " + runtime.bridge.describe());
        lines.add("Ownership: " + runtime.registry.size() + " known chunk(s), epoch "
                + runtime.registry.currentEpoch() + ", local "
                + runtime.registry.chunksOwnedBy(runtime.nodeId).size());
        lines.add("Deferred mutations: " + runtime.dispatcher.deferredCount() + " (dropped "
                + runtime.dispatcher.deferredDropped() + ")");
        lines.add("Metrics: " + runtime.metrics.summary());
        lines.add("Applied mixins: " + MixinProbe.applied());
        if (runtime.dispatcher.peerStatuses().size() > 0) {
            lines.add("Peer status: " + runtime.dispatcher.peerStatuses());
        }
        return lines;
    }
}
