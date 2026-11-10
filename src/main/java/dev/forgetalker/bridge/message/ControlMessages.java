package dev.forgetalker.bridge.message;

import java.util.List;
import java.util.Objects;

/**
 * Control-plane frames: session handshake, liveness, peer discovery, status and orderly shutdown.
 *
 * <h2>Operational purpose</h2>
 * These frames never mutate the world. They establish <em>who</em> is on the bus, <em>whether</em>
 * the bus is healthy, and <em>which endpoints</em> peers can be reached at directly.
 *
 * <h2>Thread safety</h2>
 * All frames are immutable records. They are decoded on a Netty event-loop thread and consumed on
 * the server thread; immutability is what makes that hand-off safe without locks.
 *
 * <h2>Cluster context</h2>
 * {@code HELLO} is sent to the MultiPaper master, which answers with {@code HELLO_ACK} (carrying the
 * initial ownership epoch and peer endpoints) or {@code REJECT}. Peer endpoints enable the direct
 * peer-to-peer data plane; when a peer link cannot be established the transport falls back to
 * routing frames through the master.
 */
public final class ControlMessages {

    /** Highest protocol version understood by this build. Sent in {@link Hello}. */
    public static final int PROTOCOL_VERSION = 1;

    private ControlMessages() {
        // Holder class.
    }

    /**
     * Reachability information for one node's peer-to-peer data port.
     *
     * @param nodeId node that listens on {@code host:port}, never {@code null}
     * @param host   hostname or IP address peers should dial, never {@code null} or blank
     * @param port   TCP port in {@code [1, 65535]}
     */
    public record PeerEndpoint(String nodeId, String host, int port) {

        /**
         * Validates the endpoint so that a malformed advertisement cannot make the transport dial
         * an arbitrary port.
         *
         * @throws IllegalArgumentException if {@code nodeId}/{@code host} are blank or the port is
         *                                  outside {@code [1, 65535]}
         */
        public PeerEndpoint {
            if (nodeId == null || nodeId.isBlank()) {
                throw new IllegalArgumentException("peer nodeId must not be blank");
            }
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("peer host must not be blank");
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("peer port out of range: " + port);
            }
        }

        /**
         * @return {@code node@host:port}, used in debug logs
         */
        @Override
        public String toString() {
            return nodeId + "@" + host + ":" + port;
        }
    }

    /**
     * First frame sent by a node after the TCP connection to the master is established.
     *
     * @param nodeId          stable id of the connecting node (persisted in
     *                        {@code config/forgetalker/node-id.txt})
     * @param clusterSecret   shared secret configured on master and nodes; compared in constant
     *                        time by the master. Never logged by Forge Talker.
     * @param modVersion      human-readable Forge Talker version, used for mismatch diagnostics
     * @param protocolVersion {@link #PROTOCOL_VERSION} of the sender
     * @param role            {@code "server"} for a Forge dedicated server; reserved for future
     *                        client-proxy nodes
     * @param peerEndpoint    port this node accepts direct peer connections on, or {@code 0} when
     *                        the peer listener is disabled
     */
    public record Hello(String nodeId,
                        String clusterSecret,
                        String modVersion,
                        int protocolVersion,
                        String role,
                        int peerEndpoint) implements BridgeMessage {

        /**
         * Validates the mandatory fields.
         *
         * @throws IllegalArgumentException for blank ids/roles or an out-of-range port
         */
        public Hello {
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(clusterSecret, "clusterSecret");
            Objects.requireNonNull(modVersion, "modVersion");
            if (role == null || role.isBlank()) {
                throw new IllegalArgumentException("role must not be blank");
            }
            if (peerEndpoint < 0 || peerEndpoint > 65535) {
                throw new IllegalArgumentException("peerEndpoint out of range: " + peerEndpoint);
            }
        }

        /**
         * @return {@link MessageType#HELLO}
         */
        @Override
        public MessageType type() {
            return MessageType.HELLO;
        }
    }

    /**
     * Master's answer to a successful {@link Hello}.
     *
     * @param masterNodeId    id of the master, used for diagnostics only
     * @param sessionId       opaque session token; logged so an operator can correlate a node
     *                        restart with the master's audit log
     * @param protocolVersion protocol version the master runs; the node refuses to continue when it
     *                        differs from {@link #PROTOCOL_VERSION}
     * @param ownershipEpoch  epoch of the ownership snapshot that follows, so the node can discard
     *                        out-of-order deltas received before the snapshot
     * @param peers           directly dialable peers, may be empty
     */
    public record HelloAck(String masterNodeId,
                           String sessionId,
                           int protocolVersion,
                           long ownershipEpoch,
                           List<PeerEndpoint> peers) implements BridgeMessage {

        /**
         * Defensive copy of {@code peers} so later mutation by the caller cannot change a frame that
         * is being encoded on another thread.
         *
         * @throws NullPointerException if any field is {@code null}
         */
        public HelloAck {
            Objects.requireNonNull(masterNodeId, "masterNodeId");
            Objects.requireNonNull(sessionId, "sessionId");
            peers = List.copyOf(Objects.requireNonNull(peers, "peers"));
        }

        /**
         * @return {@link MessageType#HELLO_ACK}
         */
        @Override
        public MessageType type() {
            return MessageType.HELLO_ACK;
        }
    }

    /**
     * Master's refusal of a session.
     *
     * @param reason    human readable cause, safe to log (never contains the secret)
     * @param retryable {@code true} for transient conditions (cluster starting, capacity) where the
     *                  node should reconnect; {@code false} for configuration errors where
     *                  reconnecting would only spam the master
     */
    public record Reject(String reason, boolean retryable) implements BridgeMessage {

        /**
         * @throws NullPointerException if {@code reason} is {@code null}
         */
        public Reject {
            Objects.requireNonNull(reason, "reason");
        }

        /**
         * @return {@link MessageType#REJECT}
         */
        @Override
        public MessageType type() {
            return MessageType.REJECT;
        }
    }

    /**
     * Liveness probe.
     *
     * @param nonce        caller-chosen value echoed by the matching {@link Pong}; used to compute
     *                     round-trip time without keeping per-probe state
     * @param sentAtMillis wall-clock send time, for operator diagnostics only (never used for
     *                     ordering, because node clocks are not synchronised)
     */
    public record Ping(long nonce, long sentAtMillis) implements BridgeMessage {

        /**
         * @return {@link MessageType#PING}
         */
        @Override
        public MessageType type() {
            return MessageType.PING;
        }
    }

    /**
     * Answer to a {@link Ping}.
     *
     * @param nonce        value copied from the probe
     * @param sentAtMillis original send time, echoed so the prober can compute round-trip latency
     */
    public record Pong(long nonce, long sentAtMillis) implements BridgeMessage {

        /**
         * @return {@link MessageType#PONG}
         */
        @Override
        public MessageType type() {
            return MessageType.PONG;
        }
    }

    /**
     * Master -&gt; node: refreshed list of directly dialable peers (sent when a node joins or
     * leaves). Nodes use it to (re)build the peer-to-peer data plane.
     *
     * @param peers peer endpoints, may be empty when this node is alone
     */
    public record PeerAdvertisement(List<PeerEndpoint> peers) implements BridgeMessage {

        /**
         * @throws NullPointerException if {@code peers} is {@code null}
         */
        public PeerAdvertisement {
            peers = List.copyOf(Objects.requireNonNull(peers, "peers"));
        }

        /**
         * @return {@link MessageType#PEER_ADVERTISEMENT}
         */
        @Override
        public MessageType type() {
            return MessageType.PEER_ADVERTISEMENT;
        }
    }

    /**
     * Requests a status snapshot from a peer.
     *
     * @param requestId       correlation id echoed in {@link StatusResponse}; chosen by the requester
     * @param requesterNodeId node that wants the snapshot. Carried in the frame because the transport
     *                        envelope deliberately does not expose the remote identity to the
     *                        dispatcher; without it a peer could not address its answer.
     */
    public record StatusRequest(long requestId, String requesterNodeId) implements BridgeMessage {

        /**
         * @throws IllegalArgumentException if {@code requesterNodeId} is blank
         */
        public StatusRequest {
            if (requesterNodeId == null || requesterNodeId.isBlank()) {
                throw new IllegalArgumentException("requesterNodeId must not be blank");
            }
        }

        /**
         * @return {@link MessageType#STATUS_REQUEST}
         */
        @Override
        public MessageType type() {
            return MessageType.STATUS_REQUEST;
        }
    }

    /**
     * Status snapshot of one node, used by {@code /forgetalker status} and by health tooling.
     *
     * @param requestId      correlation id from the {@link StatusRequest}
     * @param nodeId         reporting node
     * @param state          transport state name ({@code CONNECTED}, {@code DEGRADED}, ...)
     * @param ownedChunks    number of chunks the reporting node believes it owns
     * @param peerCount      number of live peer links
     * @param ownershipEpoch highest ownership epoch the reporting node applied
     */
    public record StatusResponse(long requestId,
                                 String nodeId,
                                 String state,
                                 int ownedChunks,
                                 int peerCount,
                                 long ownershipEpoch) implements BridgeMessage {

        /**
         * @return {@link MessageType#STATUS_RESPONSE}
         */
        @Override
        public MessageType type() {
            return MessageType.STATUS_RESPONSE;
        }
    }

    /**
     * Orderly session close.
     *
     * @param reason human readable cause, logged at INFO
     */
    public record Goodbye(String reason) implements BridgeMessage {

        /**
         * @throws NullPointerException if {@code reason} is {@code null}
         */
        public Goodbye {
            Objects.requireNonNull(reason, "reason");
        }

        /**
         * @return {@link MessageType#GOODBYE}
         */
        @Override
        public MessageType type() {
            return MessageType.GOODBYE;
        }
    }
}
