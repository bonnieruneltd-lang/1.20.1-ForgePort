package dev.forgetalker.bridge.message;

import dev.forgetalker.cluster.ChunkCoordinate;
import dev.forgetalker.cluster.ClusterNodeId;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Ownership-plane frames: the master's authoritative chunk table, its deltas and node interest.
 *
 * <h2>Operational purpose</h2>
 * MultiPaper assigns every chunk to exactly one node. Forge Talker mirrors those assignments so
 * that mixin guards can decide, without a network round trip, whether a mutation is local.
 *
 * <h2>Thread safety</h2>
 * Immutable records. Decoded on the Netty event loop, applied to
 * {@link dev.forgetalker.cluster.ChunkOwnershipRegistry} on the server thread, which preserves
 * frame ordering (last writer from the master wins).
 *
 * <h2>Cluster context</h2>
 * The master is the only writer of ownership. {@link ChunkInterest} frames are <em>hints</em>
 * ("my player is here, assign this chunk to me"); a node must never mutate the registry from local
 * interest alone, otherwise two nodes could believe they own the same chunk.
 */
public final class OwnershipMessages {

    private OwnershipMessages() {
        // Holder class.
    }

    /**
     * Why a node wants (or no longer wants) a chunk.
     */
    public enum InterestKind {

        /** A player moved into the chunk / the chunk was loaded for gameplay. */
        CLAIM("claim"),

        /** The chunk was unloaded; the master may reassign it to another node. */
        RELEASE("release");

        private final String wireId;

        /**
         * @param wireId stable JSON discriminator
         */
        InterestKind(String wireId) {
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
        public static InterestKind fromWireId(String wireId) {
            for (InterestKind kind : values()) {
                if (kind.wireId.equals(wireId)) {
                    return kind;
                }
            }
            return null;
        }
    }

    /**
     * One row of the master's ownership table.
     *
     * @param chunk chunk this entry describes, never {@code null}
     * @param owner node that currently owns the chunk, never {@code null}
     */
    public record ChunkOwnershipEntry(ChunkCoordinate chunk, ClusterNodeId owner) {

        /**
         * @throws NullPointerException if either component is {@code null}
         */
        public ChunkOwnershipEntry {
            Objects.requireNonNull(chunk, "chunk");
            Objects.requireNonNull(owner, "owner");
        }
    }

    /**
     * Full ownership table for the cluster, sent right after a successful handshake.
     *
     * @param epoch   master epoch of this snapshot; used by the registry to discard stale data
     * @param entries all assignments, may be empty
     */
    public record OwnershipSnapshot(long epoch, List<ChunkOwnershipEntry> entries) implements BridgeMessage {

        /**
         * Copies {@code entries} so the frame cannot be mutated after it is handed to the transport.
         *
         * @throws NullPointerException if {@code entries} is {@code null}
         */
        public OwnershipSnapshot {
            entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        }

        /**
         * Converts the frame into the map shape expected by
         * {@link dev.forgetalker.cluster.ChunkOwnershipRegistry#applySnapshot(long, Map)}.
         *
         * @return an immutable chunk-to-node map, never {@code null}
         * @implNote If the master ever sends duplicate keys (it must not), the last entry wins;
         *         throwing here would tear down a whole cluster session over one bad row.
         */
        public Map<ChunkCoordinate, ClusterNodeId> toMap() {
            Map<ChunkCoordinate, ClusterNodeId> result = new java.util.HashMap<>(entries.size());
            for (ChunkOwnershipEntry entry : entries) {
                result.put(entry.chunk(), entry.owner());
            }
            return Map.copyOf(result);
        }

        /**
         * @return {@link MessageType#OWNERSHIP_SNAPSHOT}
         */
        @Override
        public MessageType type() {
            return MessageType.OWNERSHIP_SNAPSHOT;
        }
    }

    /**
     * Single assignment change.
     *
     * @param epoch master epoch of this delta
     * @param chunk affected chunk, never {@code null}
     * @param owner new owner, or {@code null} when the chunk was released (nobody owns it)
     */
    public record OwnershipUpdate(long epoch, ChunkCoordinate chunk, ClusterNodeId owner)
            implements BridgeMessage {

        /**
         * @throws NullPointerException if {@code chunk} is {@code null}
         */
        public OwnershipUpdate {
            Objects.requireNonNull(chunk, "chunk");
        }

        /**
         * @return {@code true} when this delta assigns an owner, {@code false} when it releases
         */
        public boolean isAssignment() {
            return owner != null;
        }

        /**
         * @return {@link MessageType#OWNERSHIP_UPDATE}
         */
        @Override
        public MessageType type() {
            return MessageType.OWNERSHIP_UPDATE;
        }
    }

    /**
     * Node -&gt; master hint that a chunk became (un)interesting for this node.
     *
     * @param chunk chunk the node is interested in, never {@code null}
     * @param kind  {@link InterestKind#CLAIM} or {@link InterestKind#RELEASE}
     */
    public record ChunkInterest(ChunkCoordinate chunk, InterestKind kind) implements BridgeMessage {

        /**
         * @throws NullPointerException if either component is {@code null}
         */
        public ChunkInterest {
            Objects.requireNonNull(chunk, "chunk");
            Objects.requireNonNull(kind, "kind");
        }

        /**
         * @return {@link MessageType#CHUNK_INTEREST}
         */
        @Override
        public MessageType type() {
            return MessageType.CHUNK_INTEREST;
        }
    }
}
