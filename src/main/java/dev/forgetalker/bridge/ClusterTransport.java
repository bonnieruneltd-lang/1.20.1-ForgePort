package dev.forgetalker.bridge;

import dev.forgetalker.bridge.message.BridgeMessage;
import dev.forgetalker.cluster.ClusterNodeId;

/**
 * Abstraction over the node's connection(s) to the rest of the cluster.
 *
 * <h2>Operational purpose</h2>
 * The bridge, dispatcher and event handlers must not care whether frames travel over the MultiPaper
 * master connection, a direct peer socket, or nowhere at all (standalone). This interface is that
 * seam. It also makes the transport replaceable: a future agent can add a Unix-domain-socket or
 * in-memory test transport without touching the bridge logic.
 *
 * <h2>Implementations</h2>
 * <ul>
 *   <li>{@link MultiPaperTcpTransport} - production Netty implementation (master link + peer links)</li>
 *   <li>{@link StandaloneTransport} - inert implementation used when the cluster is disabled</li>
 * </ul>
 *
 * <h2>Threading contract</h2>
 * {@link #send(BridgeMessage)} and {@link #sendTo(ClusterNodeId, BridgeMessage)} are safe to call
 * from any thread; implementations must enqueue and never block the caller (which is frequently the
 * server thread). {@link TransportListener} callbacks arrive on Netty threads.
 */
public interface ClusterTransport extends AutoCloseable {

    /**
     * Starts the transport. Idempotent; a second call while running must be ignored.
     */
    void start();

    /**
     * Stops the transport, attempting to send an orderly GOODBYE first.
     *
     * @param reason reason logged locally and sent to the master, never {@code null}
     */
    void stop(String reason);

    /**
     * Broadcasts a frame to the master and (when possible) to every peer.
     *
     * @param message frame to send, never {@code null}
     */
    void send(BridgeMessage message);

    /**
     * Sends a frame to one specific node, using a direct peer link when available and falling back
     * to routing through the master.
     *
     * @param target destination node, never {@code null}
     * @param message frame to send, never {@code null}
     */
    void sendTo(ClusterNodeId target, BridgeMessage message);

    /**
     * @return the current lifecycle state, never {@code null}
     */
    TransportState state();

    /**
     * @return a one-line description for logs/status (target, peers, queue depth)
     */
    String describe();

    /**
     * @return the number of live direct peer links; {@code 0} for standalone transports
     */
    default int peerCount() {
        return 0;
    }

    /**
     * @return the ids of the nodes this transport currently has a direct link with; empty by default
     */
    default java.util.Set<String> peerNodeIds() {
        return java.util.Set.of();
    }

    /**
     * Registers a listener. Must be called before {@link #start()} for deterministic ordering.
     *
     * @param listener listener to add, never {@code null}
     */
    void addListener(TransportListener listener);

    /**
     * Stops the transport; equivalent to {@code stop("closed")}.
     */
    @Override
    default void close() {
        stop("closed");
    }
}
