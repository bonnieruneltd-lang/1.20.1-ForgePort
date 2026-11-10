package dev.forgetalker.dispatch;

import dev.forgetalker.bridge.ClusterBridge;
import dev.forgetalker.bridge.ClusterMetrics;
import dev.forgetalker.bridge.TransportListener;
import dev.forgetalker.bridge.TransportState;
import dev.forgetalker.bridge.message.BridgeMessage;
import dev.forgetalker.bridge.message.ControlMessages;
import dev.forgetalker.bridge.message.ControlMessages.HelloAck;
import dev.forgetalker.bridge.message.ControlMessages.StatusRequest;
import dev.forgetalker.bridge.message.ControlMessages.StatusResponse;
import dev.forgetalker.bridge.message.MutationMessages.BlockMutation;
import dev.forgetalker.bridge.message.MutationMessages.ClusterEvent;
import dev.forgetalker.bridge.message.MutationMessages.EntitySpawn;
import dev.forgetalker.bridge.message.MutationMessages.PayloadRelay;
import dev.forgetalker.bridge.message.MutationMessages.PlayerPresence;
import dev.forgetalker.bridge.message.MutationMessages.RoutedFrame;
import dev.forgetalker.bridge.message.OwnershipMessages.OwnershipSnapshot;
import dev.forgetalker.bridge.message.OwnershipMessages.OwnershipUpdate;
import dev.forgetalker.cluster.ChunkCoordinate;
import dev.forgetalker.cluster.ChunkOwnershipGuard;
import dev.forgetalker.cluster.ClusterNodeId;
import dev.forgetalker.cluster.OwnershipPolicy;
import dev.forgetalker.cluster.MutationVerdict;
import dev.forgetalker.config.RuntimeSettings;
import net.minecraft.core.BlockPos;
import net.minecraft.core.registries.Registries;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.EntityType;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.state.BlockState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Applies inbound cluster frames to this node's world.
 *
 * <h2>Operational purpose</h2>
 * The transport delivers frames on a Netty thread. This dispatcher is the single place where they
 * cross into the server thread, in strict arrival order, using {@link MinecraftServer#execute}. That
 * ordering matters: ownership deltas must be applied before a mutation that depends on them, and two
 * block changes to the same position must keep their relative order.
 *
 * <h2>Defence in depth</h2>
 * Ownership is re-checked on arrival ({@link OwnershipPolicy#evaluateRelayDelivery}) and the
 * {@link ChunkOwnershipGuard} is opened in "remote apply" mode for the duration of the write. A
 * frame that was addressed to this node while the master already reassigned the chunk is dropped
 * with a debug log instead of producing a ghost block.
 *
 * <h2>Thread safety</h2>
 * {@link #onMessage(BridgeMessage)} and {@link #onStateChanged} may be called from any thread and
 * never touch world state directly. All apply methods run on the server thread. The deferred queue
 * and the peer status map are confined to the server thread (the status map is concurrent because
 * the command reads it from the command thread).
 */
public final class PeerMessageDispatcher implements TransportListener {

    private static final Logger LOGGER = LogManager.getLogger("ForgeTalker/Dispatcher");

    private final Supplier<MinecraftServer> serverSupplier;
    private final ClusterBridge bridge;
    private final ChunkOwnershipGuard guard;
    private final ClusterMetrics metrics;
    private final Supplier<RuntimeSettings> settings;
    private final RelayedPayloadDispatcher payloadDispatcher = new RelayedPayloadDispatcher();
    private final DeferredMutationQueue<PendingMutation> deferred;
    private final Map<String, StatusResponse> peerStatuses = new ConcurrentHashMap<>();
    private final AtomicLong statusRequestId = new AtomicLong();

    private volatile String lastTransportState = "STANDALONE (not started)";

    /**
     * @param serverSupplier supplies the running server, may return {@code null} during shutdown
     * @param bridge         cluster facade, never {@code null}
     * @param guard          ownership guard, never {@code null}
     * @param metrics        counters, never {@code null}
     * @param settings       settings supplier, never {@code null}
     */
    public PeerMessageDispatcher(Supplier<MinecraftServer> serverSupplier,
                                 ClusterBridge bridge,
                                 ChunkOwnershipGuard guard,
                                 ClusterMetrics metrics,
                                 Supplier<RuntimeSettings> settings) {
        this.serverSupplier = Objects.requireNonNull(serverSupplier, "serverSupplier");
        this.bridge = Objects.requireNonNull(bridge, "bridge");
        this.guard = Objects.requireNonNull(guard, "guard");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.deferred = new DeferredMutationQueue<>(settings.get().deferredMutationQueueLimit(),
                this::isChunkLoaded);
    }

    /**
     * Records the transport state and drops ownership data when the session is no longer
     * authoritative.
     *
     * @param state  new state, never {@code null}
     * @param detail human readable detail, never {@code null}
     * @implNote Called on a Netty thread. Clearing the registry here (instead of on the server
     *         thread) is safe because {@link dev.forgetalker.cluster.ChunkOwnershipRegistry} is
     *         concurrent, and it guarantees that guards stop enforcing stale ownership immediately.
     */
    @Override
    public void onStateChanged(TransportState state, String detail) {
        lastTransportState = state + " (" + detail + ")";
        if (!state.authoritative()) {
            bridge.registry().clear();
            deferred.clear();
        }
    }

    /**
     * Hands an inbound frame to the server thread.
     *
     * @param message decoded frame, never {@code null}
     */
    @Override
    public void onMessage(BridgeMessage message) {
        MinecraftServer server = serverSupplier.get();
        if (server == null) {
            LOGGER.debug("Dropping {} frame: server is not running", message.type());
            return;
        }
        server.execute(() -> handleInbound(message));
    }

    /**
     * Applies one frame on the server thread.
     *
     * @param message decoded frame, never {@code null}
     * @implNote {@code instanceof} chains are used instead of a pattern-matching {@code switch}
     *         because switch patterns are still a preview feature in Java 17 and the build must not
     *         enable preview flags for a production mod.
     */
    private void handleInbound(BridgeMessage message) {
        if (message instanceof OwnershipSnapshot snapshot) {
            if (bridge.registry().applySnapshot(snapshot.epoch(), snapshot.toMap())) {
                metrics.recordOwnershipSnapshot();
                LOGGER.info("Applied ownership snapshot epoch {} with {} entries",
                        snapshot.epoch(), snapshot.entries().size());
            } else {
                LOGGER.debug("Ignored stale ownership snapshot epoch {}", snapshot.epoch());
            }
            return;
        }
        if (message instanceof OwnershipUpdate update) {
            if (bridge.registry().applyUpdate(update.epoch(), update.chunk(), update.owner())) {
                metrics.recordOwnershipUpdate();
                if (settings.get().debugLogging()) {
                    LOGGER.debug("Ownership {} for {} at epoch {}",
                            update.isAssignment() ? "assigned" : "released",
                            update.chunk(), update.epoch());
                }
            }
            return;
        }
        if (message instanceof BlockMutation mutation) {
            applyOrDefer(mutation.chunk(), mutation);
            return;
        }
        if (message instanceof EntitySpawn spawn) {
            applyOrDefer(spawn.chunk(), spawn);
            return;
        }
        if (message instanceof PayloadRelay relay) {
            payloadDispatcher.deliver(server(), relay, settings.get().relayPayloadHopLimit());
            return;
        }
        if (message instanceof PlayerPresence presence) {
            LOGGER.debug("Player {} {} on {}", presence.playerName(),
                    presence.kind().wireId(), presence.nodeId());
            return;
        }
        if (message instanceof ClusterEvent event) {
            LOGGER.debug("Cluster event {} at {} ({},{},{}) detail={}", event.kind(),
                    event.dimension(), event.x(), event.y(), event.z(), event.detail());
            return;
        }
        if (message instanceof RoutedFrame routed) {
            if (bridge.localNode().value().equals(routed.targetNode())) {
                handleInbound(routed.inner());
            } else {
                LOGGER.warn("Received routed frame for {} on {}; dropping",
                        routed.targetNode(), bridge.localNode());
            }
            return;
        }
        if (message instanceof StatusRequest request) {
            replyWithStatus(request);
            return;
        }
        if (message instanceof StatusResponse response) {
            peerStatuses.put(response.nodeId(), response);
            return;
        }
        if (message instanceof HelloAck ack) {
            LOGGER.debug("Session acknowledged by {} (epoch {})",
                    ack.masterNodeId(), ack.ownershipEpoch());
            return;
        }
        if (message instanceof ControlMessages.Reject reject) {
            LOGGER.debug("Session rejected: {}", reject.reason());
            return;
        }
        if (message instanceof ControlMessages.Goodbye goodbye) {
            LOGGER.debug("Peer said goodbye: {}", goodbye.reason());
            return;
        }
        if (message instanceof ControlMessages.PeerAdvertisement advertisement) {
            LOGGER.debug("Peer advertisement with {} endpoints", advertisement.peers().size());
            return;
        }
        // Hello, Ping, Pong and ChunkInterest are transport/master concerns; nothing to apply.
    }

    /**
     * Applies a mutation immediately when its chunk is loaded, otherwise defers it.
     *
     * @param chunk   target chunk, never {@code null}
     * @param message frame to apply, never {@code null}
     */
    private void applyOrDefer(ChunkCoordinate chunk, BridgeMessage message) {
        PendingMutation pending = new PendingMutation(chunk, message);
        if (!isChunkLoaded(pending)) {
            if (deferred.offer(pending)) {
                LOGGER.debug("Deferred queue full; dropped the oldest mutation for {}", chunk);
            }
            return;
        }
        applyNow(pending);
    }

    /**
     * Applies one pending mutation.
     *
     * @param pending validated mutation, never {@code null}
     */
    private void applyNow(PendingMutation pending) {
        if (pending.message() instanceof BlockMutation mutation) {
            applyBlockMutation(mutation);
        } else if (pending.message() instanceof EntitySpawn spawn) {
            applyEntitySpawn(spawn);
        }
    }

    /**
     * Writes a relayed block state into the world.
     *
     * @param mutation frame to apply, never {@code null}
     */
    private void applyBlockMutation(BlockMutation mutation) {
        ServerLevel level = levelOf(mutation.chunk());
        if (level == null) {
            LOGGER.warn("Dropping block mutation for unknown dimension {}", mutation.chunk().dimension());
            return;
        }
        MutationVerdict verdict = OwnershipPolicy.evaluateRelayDelivery(true, bridge.localNode(),
                bridge.registry().ownerOf(mutation.chunk()));
        if (!verdict.isAllowed()) {
            LOGGER.debug("Dropping block mutation for {}: {}", mutation.chunk(), verdict.reason());
            return;
        }
        if (level.isOutsideBuildHeight(mutation.y())) {
            LOGGER.warn("Dropping block mutation outside build height: y={} in {}",
                    mutation.y(), mutation.chunk().dimension());
            return;
        }
        BlockPos pos = new BlockPos(mutation.x(), mutation.y(), mutation.z());
        BlockState state = Block.stateById(mutation.blockStateId());
        guard.enterRemoteApply();
        try {
            level.setBlock(pos, state, mutation.flags());
        } finally {
            guard.exitRemoteApply();
        }
        if (settings.get().debugLogging()) {
            LOGGER.debug("Applied relayed block mutation at {} from {}", pos, mutation.originNode());
        }
    }

    /**
     * Spawns a relayed entity, de-duplicating by UUID and restoring NBT when supplied.
     *
     * @param spawn frame to apply, never {@code null}
     */
    private void applyEntitySpawn(EntitySpawn spawn) {
        ServerLevel level = levelOf(spawn.chunk());
        if (level == null) {
            LOGGER.warn("Dropping entity spawn for unknown dimension {}", spawn.chunk().dimension());
            return;
        }
        if (level.getEntity(spawn.entityUuid()) != null) {
            LOGGER.debug("Skipping duplicate entity spawn {}", spawn.entityUuid());
            return;
        }
        Optional<Entity> created = createEntity(level, spawn);
        if (created.isEmpty()) {
            LOGGER.warn("Could not create entity {} for spawn {}", spawn.entityTypeId(), spawn.entityUuid());
            return;
        }
        Entity entity = created.get();
        entity.moveTo(spawn.x(), spawn.y(), spawn.z(), spawn.yaw(), spawn.pitch());
        guard.enterRemoteApply();
        try {
            if (!level.addFreshEntity(entity)) {
                LOGGER.debug("Level refused relayed entity spawn {}", spawn.entityUuid());
                return;
            }
        } finally {
            guard.exitRemoteApply();
        }
        metrics.recordRelayedEntitySpawn();
        if (settings.get().debugLogging()) {
            LOGGER.debug("Spawned relayed entity {} at {}, {}, {}",
                    spawn.entityTypeId(), spawn.x(), spawn.y(), spawn.z());
        }
    }

    /**
     * Builds the entity described by an {@link EntitySpawn} frame.
     *
     * @param level owning level, never {@code null}
     * @param spawn frame describing the entity, never {@code null}
     * @return the created entity, or empty when the type is unknown or the NBT is corrupt
     */
    private Optional<Entity> createEntity(ServerLevel level, EntitySpawn spawn) {
        byte[] nbt = spawn.nbt();
        if (nbt != null) {
            try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(nbt))) {
                CompoundTag tag = NbtIo.read(input);
                if (tag != null) {
                    return EntityType.create(tag, level);
                }
            } catch (IOException | RuntimeException e) {
                LOGGER.warn("Ignoring corrupt NBT on relayed spawn {}: {}",
                        spawn.entityUuid(), e.toString());
            }
        }
        Optional<EntityType<?>> type = EntityType.byString(spawn.entityTypeId());
        return type.map(entityType -> entityType.create(level));
    }

    /**
     * Answers a peer's status request with this node's snapshot.
     *
     * @param request request frame, never {@code null}
     */
    private void replyWithStatus(StatusRequest request) {
        ClusterNodeId requester;
        try {
            requester = ClusterNodeId.parse(request.requesterNodeId());
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Dropping status request with invalid requester '{}'",
                    request.requesterNodeId());
            return;
        }
        bridge.sendTo(requester, new StatusResponse(request.requestId(), bridge.localNode().value(),
                lastTransportState,
                bridge.registry().chunksOwnedBy(bridge.localNode()).size(),
                bridge.transport().peerCount(),
                bridge.registry().currentEpoch()));
    }

    /**
     * Applies deferred mutations whose chunks have finished loading.
     *
     * <h2>Threading</h2>
     * Called from {@code ServerTickEvent} on the server thread. The budget bounds the work per tick
     * so a large backlog cannot stall the server; the remainder is retried next tick.
     */
    public void drainDeferred() {
        int applied = deferred.drain(pending -> {
            applyNow(pending);
            metrics.recordDeferredApplied();
        }, 256);
        if (applied > 0 && settings.get().debugLogging()) {
            LOGGER.debug("Applied {} deferred mutations ({} still queued)", applied, deferred.size());
        }
    }

    /**
     * @param pending pending mutation, never {@code null}
     * @return {@code true} when the target chunk is loaded on this node
     */
    private boolean isChunkLoaded(PendingMutation pending) {
        ServerLevel level = levelOf(pending.chunk());
        if (level == null) {
            return false;
        }
        return level.isLoaded(blockPosOf(pending));
    }

    /**
     * @param pending pending mutation, never {@code null}
     * @return a position inside the target chunk, used for the loaded check
     */
    private BlockPos blockPosOf(PendingMutation pending) {
        if (pending.message() instanceof BlockMutation mutation) {
            return new BlockPos(mutation.x(), mutation.y(), mutation.z());
        }
        EntitySpawn spawn = (EntitySpawn) pending.message();
        return BlockPos.containing(spawn.x(), spawn.y(), spawn.z());
    }

    /**
     * Resolves a dimension key to a level.
     *
     * @param chunk chunk naming the dimension, never {@code null}
     * @return the level, or {@code null} when this node does not run that dimension
     */
    private ServerLevel levelOf(ChunkCoordinate chunk) {
        ResourceLocation id = ResourceLocation.tryParse(chunk.dimension());
        if (id == null) {
            return null;
        }
        MinecraftServer server = serverSupplier.get();
        if (server == null) {
            return null;
        }
        return server.getLevel(ResourceKey.create(Registries.DIMENSION, id));
    }

    /**
     * @return the running server, never {@code null} while called from the server thread
     * @throws IllegalStateException when the server is already gone
     */
    private MinecraftServer server() {
        MinecraftServer server = serverSupplier.get();
        if (server == null) {
            throw new IllegalStateException("Server is not running");
        }
        return server;
    }

    /**
     * @return the last observed transport state with its detail, never {@code null}
     */
    public String lastTransportState() {
        return lastTransportState;
    }

    /**
     * @return the number of mutations currently deferred
     */
    public int deferredCount() {
        return deferred.size();
    }

    /**
     * @return total mutations dropped because the deferral queue was full
     */
    public long deferredDropped() {
        return deferred.droppedCount();
    }

    /**
     * @return an immutable view of the latest status reported by each peer
     */
    public Map<String, StatusResponse> peerStatuses() {
        return Map.copyOf(peerStatuses);
    }

    /**
     * Asks every directly linked peer for a fresh status snapshot.
     *
     * <h2>Threading</h2>
     * Called from the command thread ({@code /forgetalker status}); replies are stored by
     * {@link #handleInbound} on the server thread and read back through {@link #peerStatuses()}.
     */
    public void requestStatusFromAllPeers() {
        long id = statusRequestId.incrementAndGet();
        for (String peerId : bridge.transport().peerNodeIds()) {
            try {
                bridge.requestStatus(ClusterNodeId.parse(peerId), id);
            } catch (IllegalArgumentException e) {
                LOGGER.debug("Skipping status request for invalid peer id '{}'", peerId);
            }
        }
    }

    /**
     * Clears runtime state on shutdown.
     */
    public void shutdown() {
        deferred.clear();
        peerStatuses.clear();
    }
}
