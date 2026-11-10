package dev.forgetalker.bridge;

/**
 * Lifecycle state of the cluster transport.
 *
 * <h2>Operational purpose</h2>
 * Guards and the event bridge need a single, cheap answer to "is ownership authoritative right
 * now?". Only {@link #CONNECTED} and {@link #DEGRADED} mean yes: after a handshake the master has
 * told us which chunks we own. In every other state the guards fall back to standalone behaviour,
 * because enforcing ownership without a snapshot would reject legitimate mutations.
 *
 * <h2>Thread safety</h2>
 * Enum constants are immutable. The transport publishes its state through a {@code volatile} field
 * and notifies listeners on state transitions from the Netty event loop.
 */
public enum TransportState {

    /** No cluster is configured; single-server behaviour. */
    STANDALONE,

    /** Configured but not connected (initial state or after a failure). */
    DISCONNECTED,

    /** TCP connect in progress. */
    CONNECTING,

    /** TCP established, handshake/HELLO_ACK in flight. */
    HANDSHAKING,

    /** Session active and authoritative. */
    CONNECTED,

    /** Session active but at least one peer link is down; the master relay is used instead. */
    DEGRADED,

    /** Transport shut down deliberately. */
    STOPPED;

    /**
     * @return {@code true} when ownership data is authoritative for this node
     */
    public boolean authoritative() {
        return this == CONNECTED || this == DEGRADED;
    }
}
