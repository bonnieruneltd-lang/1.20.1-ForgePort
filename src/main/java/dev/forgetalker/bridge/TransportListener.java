package dev.forgetalker.bridge;

import dev.forgetalker.bridge.message.BridgeMessage;

/**
 * Callback interface for transport lifecycle and inbound frames.
 *
 * <h2>Threading contract (important for future agents)</h2>
 * Every method is invoked on a <em>Netty event-loop thread</em>, never on the server thread. A
 * listener that needs world access must therefore hop threads, e.g.
 * {@code server.execute(() -> ...)}. {@code PeerMessageDispatcher} exists precisely to do that and
 * should be the only listener in production code.
 *
 * <h2>Cluster context</h2>
 * Frames delivered here have already been decoded and authenticated by the transport; they are not
 * yet filtered against local ownership. The dispatcher performs that second check.
 */
public interface TransportListener {

    /**
     * Called when the transport changes state.
     *
     * @param state  new state, never {@code null}
     * @param detail human readable explanation (peer, reason, error), never {@code null}
     */
    void onStateChanged(TransportState state, String detail);

    /**
     * Called for every inbound frame that is not consumed by the transport itself (handshake,
     * heartbeat and peer discovery are handled internally).
     *
     * @param message decoded frame, never {@code null}
     */
    void onMessage(BridgeMessage message);
}
