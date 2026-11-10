package dev.forgetalker.bridge.message;

/**
 * Kinds of frames exchanged on the Forge Talker cluster bus.
 *
 * <h2>Operational purpose</h2>
 * The {@link #wireId()} string is the {@code "type"} discriminator of every JSON frame, so the
 * wire format stays human-readable (a captured frame can be understood without the code) while
 * remaining stable if enum constants are reordered. <b>Never rename a wire id</b>: a rolling
 * cluster upgrade would otherwise drop every frame from older nodes.
 *
 * <h2>Thread safety</h2>
 * Enums are immutable; safe from the Netty event loop and the server thread.
 */
public enum MessageType {

    /** Node -&gt; master: opens a session and authenticates. */
    HELLO("hello"),

    /** Master -&gt; node: session accepted, carries the ownership epoch and the peer list. */
    HELLO_ACK("hello_ack"),

    /** Master -&gt; node: session refused (bad secret, version mismatch, cluster full). */
    REJECT("reject"),

    /** Either direction: liveness probe. */
    PING("ping"),

    /** Either direction: answer to {@link #PING}. */
    PONG("pong"),

    /** Master -&gt; node: full ownership table. */
    OWNERSHIP_SNAPSHOT("ownership_snapshot"),

    /** Master -&gt; node: single ownership delta. */
    OWNERSHIP_UPDATE("ownership_update"),

    /** Node -&gt; master: this node wants (or no longer wants) a chunk. */
    CHUNK_INTEREST("chunk_interest"),

    /** Node -&gt; peer: apply this block change on the owning node. */
    BLOCK_MUTATION("block_mutation"),

    /** Node -&gt; peer: spawn this entity on the owning node. */
    ENTITY_SPAWN("entity_spawn"),

    /** Node -&gt; peer: re-deliver a modded custom payload to local players. */
    PAYLOAD_RELAY("payload_relay"),

    /** Node -&gt; cluster: a player joined or left this node. */
    PLAYER_PRESENCE("player_presence"),

    /** Node -&gt; cluster: informational server event (break, entity join, ...). */
    CLUSTER_EVENT("cluster_event"),

    /** Master -&gt; node: direct peer endpoints for the peer-to-peer data plane. */
    PEER_ADVERTISEMENT("peer_advertisement"),

    /** Node -&gt; master: wraps a frame that must be forwarded to another node. */
    ROUTED_FRAME("routed_frame"),

    /** Node -&gt; peer: request a status snapshot. */
    STATUS_REQUEST("status_request"),

    /** Peer -&gt; node: status snapshot answer. */
    STATUS_RESPONSE("status_response"),

    /** Either direction: orderly session close with a reason. */
    GOODBYE("goodbye");

    private final String wireId;

    /**
     * @param wireId stable JSON discriminator; must never change once released
     */
    MessageType(String wireId) {
        this.wireId = wireId;
    }

    /**
     * @return the stable JSON discriminator written to the wire
     */
    public String wireId() {
        return wireId;
    }

    /**
     * Resolves a wire id back to its enum constant.
     *
     * @param wireId value read from a frame, may be {@code null}
     * @return the matching type, or {@code null} when the id is unknown (forward compatibility:
     *         unknown frames are ignored rather than treated as fatal)
     */
    public static MessageType fromWireId(String wireId) {
        if (wireId == null) {
            return null;
        }
        for (MessageType type : values()) {
            if (type.wireId.equals(wireId)) {
                return type;
            }
        }
        return null;
    }
}
