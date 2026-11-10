package dev.forgetalker.bridge;

import dev.forgetalker.bridge.message.BridgeMessage;
import dev.forgetalker.bridge.message.ControlMessages.StatusRequest;
import dev.forgetalker.bridge.message.MutationMessages.BlockMutation;
import dev.forgetalker.bridge.message.MutationMessages.ClusterEvent;
import dev.forgetalker.bridge.message.MutationMessages.EntitySpawn;
import dev.forgetalker.bridge.message.MutationMessages.PayloadRelay;
import dev.forgetalker.bridge.message.MutationMessages.PlayerPresence;
import dev.forgetalker.bridge.message.OwnershipMessages.ChunkInterest;
import dev.forgetalker.bridge.message.OwnershipMessages.InterestKind;
import dev.forgetalker.cluster.ChunkCoordinate;
import dev.forgetalker.cluster.ChunkOwnershipRegistry;
import dev.forgetalker.cluster.ClusterNodeId;
import dev.forgetalker.config.RuntimeSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Facade that the rest of the mod talks to when it wants to use the cluster.
 *
 * <h2>Operational purpose</h2>
 * Keeps the event bridge, mixins and command free of protocol details: they call
 * {@code broadcastPresence(...)} or {@code relayBlockMutation(...)} and never construct a transport,
 * a codec or a Netty channel. It also hosts the single definition of "is the cluster authoritative",
 * which is what the ownership guards depend on.
 *
 * <h2>Thread safety</h2>
 * All methods are safe from any thread. Sending is delegated to {@link ClusterTransport}, which
 * enqueues internally; state queries read {@code volatile} fields. The bridge holds no mutable
 * per-tick state of its own apart from the started flag.
 *
 * <h2>Cluster context</h2>
 * When the transport is not authoritative (standalone, connecting, disconnected) every helper
 * returns early and only increments nothing. Callers may therefore call them unconditionally.
 */
public final class ClusterBridge {

    private static final Logger LOGGER = LogManager.getLogger("ForgeTalker/Bridge");

    private final RuntimeSettings settings;
    private final ClusterNodeId localNode;
    private final ChunkOwnershipRegistry registry;
    private final ClusterMetrics metrics;
    private final ClusterTransport transport;
    private final AtomicBoolean started = new AtomicBoolean();

    /**
     * @param settings  settings snapshot, never {@code null}
     * @param localNode this node's id, never {@code null}
     * @param registry  ownership mirror, never {@code null}
     * @param metrics   counters, never {@code null}
     * @param transport transport implementation, never {@code null}
     */
    public ClusterBridge(RuntimeSettings settings,
                         ClusterNodeId localNode,
                         ChunkOwnershipRegistry registry,
                         ClusterMetrics metrics,
                         ClusterTransport transport) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.localNode = Objects.requireNonNull(localNode, "localNode");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    /**
     * Starts the transport exactly once.
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            transport.start();
        }
    }

    /**
     * Stops the transport and clears the ownership mirror, so a later standalone period behaves like
     * a normal single-node server.
     *
     * @param reason reason logged and sent to the master, never {@code null}
     */
    public void stop(String reason) {
        if (started.compareAndSet(true, false)) {
            transport.stop(reason);
        }
        registry.clear();
    }

    /**
     * @return {@code true} when ownership data is authoritative and guards should enforce it
     */
    public boolean isClusterActive() {
        return transport.state().authoritative();
    }

    /**
     * @return this node's id, never {@code null}
     */
    public ClusterNodeId localNode() {
        return localNode;
    }

    /**
     * @return the ownership mirror, never {@code null}
     */
    public ChunkOwnershipRegistry registry() {
        return registry;
    }

    /**
     * @return the metric counters, never {@code null}
     */
    public ClusterMetrics metrics() {
        return metrics;
    }

    /**
     * @return the underlying transport, never {@code null}
     */
    public ClusterTransport transport() {
        return transport;
    }

    /**
     * @return the settings the bridge was created with, never {@code null}
     */
    public RuntimeSettings settings() {
        return settings;
    }

    /**
     * Tells the master that a chunk became interesting (or uninteresting) for this node.
     *
     * @param chunk chunk in question, never {@code null}
     * @param kind  {@link InterestKind#CLAIM} or {@link InterestKind#RELEASE}
     * @implNote Called from {@code ChunkEvent.Load/Unload} on the server thread; intentionally
     *         fire-and-forget so a slow master cannot stall chunk loading.
     */
    public void announceInterest(ChunkCoordinate chunk, InterestKind kind) {
        if (!settings.announceChunkInterest() || !isClusterActive()) {
            return;
        }
        transport.send(new ChunkInterest(chunk, kind));
    }

    /**
     * Sends a block mutation to the node that owns the chunk.
     *
     * @param mutation mutation to forward, never {@code null}
     */
    public void relayBlockMutation(BlockMutation mutation) {
        if (!isClusterActive()) {
            return;
        }
        ClusterNodeId owner = registry.ownerOf(mutation.chunk());
        if (owner == null) {
            LOGGER.debug("Dropping block mutation for unowned chunk {}", mutation.chunk());
            return;
        }
        transport.sendTo(owner, mutation);
        metrics.recordRelayedBlockMutation();
    }

    /**
     * Sends an entity spawn to the node that owns the entity's chunk.
     *
     * @param spawn spawn request, never {@code null}
     */
    public void relayEntitySpawn(EntitySpawn spawn) {
        if (!isClusterActive()) {
            return;
        }
        ClusterNodeId owner = registry.ownerOf(spawn.chunk());
        if (owner == null) {
            LOGGER.debug("Dropping entity spawn for unowned chunk {}", spawn.chunk());
            return;
        }
        transport.sendTo(owner, spawn);
        metrics.recordRelayedEntitySpawn();
    }

    /**
     * Broadcasts a modded payload to every peer.
     *
     * @param relay payload frame, never {@code null}
     */
    public void broadcastPayload(PayloadRelay relay) {
        if (!isClusterActive()) {
            return;
        }
        transport.send(relay);
        metrics.recordRelayedPayload();
    }

    /**
     * Broadcasts a player presence change.
     *
     * @param presence presence frame, never {@code null}
     */
    public void broadcastPresence(PlayerPresence presence) {
        if (!isClusterActive()) {
            return;
        }
        transport.send(presence);
    }

    /**
     * Broadcasts an informational cluster event.
     *
     * @param event event frame, never {@code null}
     */
    public void broadcastEvent(ClusterEvent event) {
        if (!isClusterActive()) {
            return;
        }
        transport.send(event);
    }

    /**
     * Asks a peer for a status snapshot.
     *
     * @param target    peer to query, never {@code null}
     * @param requestId correlation id, echoed by the peer
     */
    public void requestStatus(ClusterNodeId target, long requestId) {
        if (!isClusterActive()) {
            return;
        }
        transport.sendTo(target, new StatusRequest(requestId));
    }

    /**
     * Sends an arbitrary frame to one node (used by the dispatcher for status replies).
     *
     * @param target  destination node, never {@code null}
     * @param message frame, never {@code null}
     */
    public void sendTo(ClusterNodeId target, BridgeMessage message) {
        if (!isClusterActive()) {
            return;
        }
        transport.sendTo(target, message);
    }

    /**
     * @return a diagnostic description of the transport, never {@code null}
     */
    public String describe() {
        return transport.describe();
    }
}
