package dev.forgetalker.bridge.message;

import dev.forgetalker.cluster.ChunkCoordinate;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Data-plane frames: instructions that change world or player state on another node.
 *
 * <h2>Operational purpose</h2>
 * These frames are the actual "talk" of Forge Talker. A node that intercepted a mutation it may not
 * perform locally turns it into one of these frames and addresses the owning node.
 *
 * <h2>Thread safety</h2>
 * Immutable records; byte arrays are defensively copied in the canonical constructors so a caller
 * cannot mutate a frame while the Netty event loop encodes it.
 *
 * <h2>Cluster context</h2>
 * Data-plane frames are addressed, not broadcast, except {@link ClusterEvent} which is purely
 * informational. {@link BlockMutation} is idempotent (applying the same state twice is harmless),
 * which makes retries safe. {@link EntitySpawn} is not retried blindly because a duplicate spawn
 * would duplicate the entity; the receiving node de-duplicates by UUID instead.
 */
public final class MutationMessages {

    /** {@link ClusterEvent} kind for a block break performed on a node. */
    public static final String EVENT_BLOCK_BREAK = "block_break";

    /** {@link ClusterEvent} kind for an entity that joined a level on a node. */
    public static final String EVENT_ENTITY_JOIN = "entity_join";

    private MutationMessages() {
        // Holder class.
    }

    /**
     * A block change that must be applied by the node that owns {@code chunk}.
     *
     * @param chunk        chunk containing the block, never {@code null}
     * @param x            world X of the block
     * @param y            world Y of the block; validated against the owning level's build height
     * @param z            world Z of the block
     * @param blockStateId global id from {@code Block.BLOCK_STATE_REGISTRY}; requires every node to
     *                     run an identical mod set, which is a hard cluster invariant
     * @param flags        vanilla {@code Block.UPDATE_*} bit flags (2 = notify clients, 3 = notify
     *                     clients and neighbours, ...)
     * @param originNode   node that originally attempted the mutation, for loop detection and logs
     */
    public record BlockMutation(ChunkCoordinate chunk,
                                int x,
                                int y,
                                int z,
                                int blockStateId,
                                int flags,
                                String originNode) implements BridgeMessage {

        /**
         * Validates the payload so a corrupt frame cannot reach the world.
         *
         * @throws IllegalArgumentException if the state id is negative or the chunk does not match
         *                                  the block coordinates
         */
        public BlockMutation {
            Objects.requireNonNull(chunk, "chunk");
            Objects.requireNonNull(originNode, "originNode");
            if (blockStateId < 0) {
                throw new IllegalArgumentException("blockStateId must be >= 0, got " + blockStateId);
            }
            ChunkCoordinate expected = ChunkCoordinate.fromBlock(chunk.dimension(), x, z);
            if (!expected.equals(chunk)) {
                throw new IllegalArgumentException(
                        "block (" + x + "," + z + ") is not inside " + chunk + " but " + expected);
            }
        }

        /**
         * @return {@link MessageType#BLOCK_MUTATION}
         */
        @Override
        public MessageType type() {
            return MessageType.BLOCK_MUTATION;
        }
    }

    /**
     * Request to spawn (or transfer) an entity on the node that owns its chunk.
     *
     * @param chunk        chunk the entity belongs to, never {@code null}
     * @param entityUuid   stable entity identity; the receiver uses it to de-duplicate retries
     * @param entityTypeId registry id such as {@code minecraft:zombie}, never {@code null}
     * @param x            world X
     * @param y            world Y
     * @param z            world Z
     * @param yaw          body yaw in degrees
     * @param pitch        head pitch in degrees
     * @param nbt          optional NBT-encoded {@code CompoundTag} (equipment, attributes, mod data);
     *                     {@code null} when the sender only has a type + position
     * @param originNode   node that requested the spawn
     */
    public record EntitySpawn(ChunkCoordinate chunk,
                              UUID entityUuid,
                              String entityTypeId,
                              double x,
                              double y,
                              double z,
                              float yaw,
                              float pitch,
                              byte[] nbt,
                              String originNode) implements BridgeMessage {

        /**
         * Defensive copy of {@code nbt} and validation of the mandatory identifiers.
         *
         * @throws NullPointerException if a mandatory field is {@code null}
         */
        public EntitySpawn {
            Objects.requireNonNull(chunk, "chunk");
            Objects.requireNonNull(entityUuid, "entityUuid");
            Objects.requireNonNull(entityTypeId, "entityTypeId");
            Objects.requireNonNull(originNode, "originNode");
            nbt = nbt == null ? null : Arrays.copyOf(nbt, nbt.length);
        }

        /**
         * @return a copy of the NBT payload, or {@code null} when none was attached
         */
        @Override
        public byte[] nbt() {
            return nbt == null ? null : Arrays.copyOf(nbt, nbt.length);
        }

        /**
         * Value-based equality including the NBT bytes.
         *
         * <h2>Why this override exists</h2>
         * The generated record {@code equals} compares the {@code byte[]} component by reference, so
         * two frames carrying identical NBT would be considered different. That breaks frame
         * de-duplication, metrics grouping and the codec round-trip tests. Arrays are compared by
         * content instead.
         *
         * @param other object to compare against, may be {@code null}
         * @return {@code true} when every field, including the NBT bytes, is equal
         */
        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof EntitySpawn that)) {
                return false;
            }
            return Double.compare(that.x, x) == 0
                    && Double.compare(that.y, y) == 0
                    && Double.compare(that.z, z) == 0
                    && Float.compare(that.yaw, yaw) == 0
                    && Float.compare(that.pitch, pitch) == 0
                    && chunk.equals(that.chunk)
                    && entityUuid.equals(that.entityUuid)
                    && entityTypeId.equals(that.entityTypeId)
                    && originNode.equals(that.originNode)
                    && Arrays.equals(nbt, that.nbt);
        }

        /**
         * Hash consistent with {@link #equals(Object)}: array content, not identity.
         *
         * @return hash of all components including the NBT bytes
         */
        @Override
        public int hashCode() {
            int result = Objects.hash(chunk, entityUuid, entityTypeId, x, y, z, yaw, pitch, originNode);
            return 31 * result + Arrays.hashCode(nbt);
        }

        /**
         * @return {@link MessageType#ENTITY_SPAWN}
         */
        @Override
        public MessageType type() {
            return MessageType.ENTITY_SPAWN;
        }
    }

    /**
     * A modded custom payload observed on one node, re-delivered to local players on another node.
     *
     * @param senderUuid uuid of the player that originally sent the payload
     * @param channel    payload channel, e.g. {@code somemod:packet}
     * @param payload    raw payload bytes (already length-prefixed as the mod expects)
     * @param hop        number of nodes the frame traversed; receivers drop anything above the
     *                   configured limit to prevent broadcast storms
     * @param originNode node that first observed the payload
     */
    public record PayloadRelay(UUID senderUuid,
                               String channel,
                               byte[] payload,
                               int hop,
                               String originNode) implements BridgeMessage {

        /**
         * Defensive copy of {@code payload} plus validation.
         *
         * @throws IllegalArgumentException if the hop count is negative or the channel is blank
         */
        public PayloadRelay {
            Objects.requireNonNull(senderUuid, "senderUuid");
            Objects.requireNonNull(originNode, "originNode");
            if (channel == null || channel.isBlank()) {
                throw new IllegalArgumentException("channel must not be blank");
            }
            if (hop < 0) {
                throw new IllegalArgumentException("hop must be >= 0");
            }
            payload = payload == null ? new byte[0] : Arrays.copyOf(payload, payload.length);
        }

        /**
         * @return a copy of the payload bytes
         */
        @Override
        public byte[] payload() {
            return Arrays.copyOf(payload, payload.length);
        }

        /**
         * @param forwardLimit maximum allowed hop count
         * @return {@code true} when this frame may be forwarded again
         */
        public boolean canForward(int forwardLimit) {
            return hop < forwardLimit;
        }

        /**
         * Value-based equality including the payload bytes.
         *
         * <h2>Why this override exists</h2>
         * The generated record {@code equals} compares {@code byte[]} by reference. Payloads are
         * relayed multiple times (broadcast + retries), so deduplicating and testing them requires
         * content comparison.
         *
         * @param other object to compare against, may be {@code null}
         * @return {@code true} when every field, including the payload bytes, is equal
         */
        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof PayloadRelay that)) {
                return false;
            }
            return hop == that.hop
                    && senderUuid.equals(that.senderUuid)
                    && channel.equals(that.channel)
                    && originNode.equals(that.originNode)
                    && Arrays.equals(payload, that.payload);
        }

        /**
         * Hash consistent with {@link #equals(Object)}: array content, not identity.
         *
         * @return hash of all components including the payload bytes
         */
        @Override
        public int hashCode() {
            int result = Objects.hash(senderUuid, channel, hop, originNode);
            return 31 * result + Arrays.hashCode(payload);
        }

        /**
         * @return {@link MessageType#PAYLOAD_RELAY}
         */
        @Override
        public MessageType type() {
            return MessageType.PAYLOAD_RELAY;
        }
    }

    /**
     * Cluster-wide player presence notification.
     */
    public enum PresenceKind {
        /** The player connected to the reporting node. */
        JOIN("join"),
        /** The player disconnected from the reporting node. */
        LEAVE("leave");

        private final String wireId;

        /**
         * @param wireId stable JSON discriminator
         */
        PresenceKind(String wireId) {
            this.wireId = wireId;
        }

        /**
         * @return the stable JSON discriminator
         */
        public String wireId() {
            return wireId;
        }

        /**
         * @param wireId value read from a frame, may be {@code null}
         * @return the matching kind, or {@code null} for unknown values
         */
        public static PresenceKind fromWireId(String wireId) {
            for (PresenceKind kind : values()) {
                if (kind.wireId.equals(wireId)) {
                    return kind;
                }
            }
            return null;
        }
    }

    /**
     * Player joined or left one node of the cluster.
     *
     * @param playerUuid uuid of the player
     * @param playerName last known display name, for logs and scoreboard mirrors
     * @param kind       {@link PresenceKind#JOIN} or {@link PresenceKind#LEAVE}
     * @param nodeId     node the player connected to / disconnected from
     */
    public record PlayerPresence(UUID playerUuid,
                                 String playerName,
                                 PresenceKind kind,
                                 String nodeId) implements BridgeMessage {

        /**
         * @throws NullPointerException if any field is {@code null}
         */
        public PlayerPresence {
            Objects.requireNonNull(playerUuid, "playerUuid");
            Objects.requireNonNull(playerName, "playerName");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(nodeId, "nodeId");
        }

        /**
         * @return {@link MessageType#PLAYER_PRESENCE}
         */
        @Override
        public MessageType type() {
            return MessageType.PLAYER_PRESENCE;
        }
    }

    /**
     * Informational event broadcast so peers can mirror per-node state (quest trackers, scoreboards,
     * anti-cheat, statistics) without querying the originating node.
     *
     * @param kind            event identifier, see {@link #EVENT_BLOCK_BREAK} and
     *                        {@link #EVENT_ENTITY_JOIN}. Do not rename existing values; peers may
     *                        persist them.
     * @param dimension       dimension resource location, never {@code null}
     * @param x               block/entity X at the time of the event
     * @param y               block/entity Y at the time of the event
     * @param z               block/entity Z at the time of the event
     * @param actorUuid       player responsible for the event, or {@code null} for environment-driven
     *                        events
     * @param detail          free-form detail (block id, entity id); must stay short
     * @param timestampMillis wall-clock time on the emitting node (diagnostics only)
     */
    public record ClusterEvent(String kind,
                               String dimension,
                               int x,
                               int y,
                               int z,
                               UUID actorUuid,
                               String detail,
                               long timestampMillis) implements BridgeMessage {

        /**
         * @throws NullPointerException if {@code kind} or {@code dimension} is {@code null}
         */
        public ClusterEvent {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(dimension, "dimension");
            detail = detail == null ? "" : detail;
        }

        /**
         * @return {@link MessageType#CLUSTER_EVENT}
         */
        @Override
        public MessageType type() {
            return MessageType.CLUSTER_EVENT;
        }
    }

    /**
     * Wrapper used when a node has no direct peer link and asks the master to forward a frame.
     *
     * @param targetNode destination node id, never {@code null} or blank
     * @param inner      frame to deliver; must not itself be a {@link RoutedFrame} (no nesting, to
     *                   keep decoding bounded)
     */
    public record RoutedFrame(String targetNode, BridgeMessage inner) implements BridgeMessage {

        /**
         * @throws IllegalArgumentException if {@code targetNode} is blank, {@code inner} is null or
         *                                  {@code inner} is another {@link RoutedFrame}
         */
        public RoutedFrame {
            if (targetNode == null || targetNode.isBlank()) {
                throw new IllegalArgumentException("targetNode must not be blank");
            }
            Objects.requireNonNull(inner, "inner");
            if (inner instanceof RoutedFrame) {
                throw new IllegalArgumentException("RoutedFrame must not be nested");
            }
        }

        /**
         * @return {@link MessageType#ROUTED_FRAME}
         */
        @Override
        public MessageType type() {
            return MessageType.ROUTED_FRAME;
        }
    }
}
