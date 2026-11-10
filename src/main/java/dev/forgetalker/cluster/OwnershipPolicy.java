package dev.forgetalker.cluster;

/**
 * Pure decision table that answers "may this node mutate this chunk/entity?".
 *
 * <h2>Operational purpose</h2>
 * Forge mods assume that the JVM they run in owns the whole world. On a MultiPaper cluster a node
 * only owns the chunks assigned to it by the master, yet {@code Level.setBlock} and
 * {@code ServerLevel.addFreshEntity} still get called for foreign chunks (AI pathing, neighbour
 * updates, modded worldgen, scheduled ticks that survived a hand-off). This class centralises the
 * resulting policy so every interception point (mixin guards, event bridge, relay dispatcher)
 * reaches an identical verdict.
 *
 * <h2>Design notes for future maintainers</h2>
 * This class intentionally contains <em>no</em> Minecraft types. That keeps the policy unit
 * testable without a game launch (see {@code src/test/java/dev/forgetalker/cluster/OwnershipPolicyTest}).
 * Minecraft-aware callers build the boolean inputs: {@code ChunkOwnershipGuard} converts a
 * {@code ServerLevel} + {@code BlockPos} into {@link ChunkCoordinate} and reads the registry.
 *
 * <p>Decision precedence, in order:
 * <ol>
 *   <li>Client side -&gt; {@link MutationDecision#ALLOW}</li>
 *   <li>Cluster inactive (standalone server, bridge down) -&gt; {@link MutationDecision#ALLOW};
 *       vanilla already discards writes to unloaded chunks</li>
 *   <li>Known foreign owner -&gt; {@link MutationDecision#RELAY}, even if the chunk is not loaded
 *       locally, because relaying is not executing</li>
 *   <li>Chunk not loaded in this node -&gt; {@link MutationDecision#REJECT} (matches vanilla
 *       behaviour of writing into a dummy chunk)</li>
 *   <li>Owner unknown -&gt; optimistic ALLOW or conservative REJECT, per configuration</li>
 *   <li>Owner is this node -&gt; {@link MutationDecision#ALLOW}</li>
 * </ol>
 *
 * <h2>Thread safety</h2>
 * Stateless utility: every method is a pure function of its arguments and safe to call from the
 * server thread (mixins/events) and the Netty event loop (relay dispatch) simultaneously.
 */
public final class OwnershipPolicy {

    /** Reason text used when the caller is a client-side level. */
    public static final String REASON_CLIENT_SIDE = "client-side level";

    /** Reason text used when no cluster session is active. */
    public static final String REASON_STANDALONE = "cluster inactive (standalone)";

    /** Reason text used when the chunk is not loaded on this node. */
    public static final String REASON_CHUNK_UNLOADED = "chunk not loaded on this node";

    /** Reason text used when the owner is unknown and optimistic mode is enabled. */
    public static final String REASON_UNKNOWN_OPTIMISTIC = "owner unknown, optimistic mode";

    /** Reason text used when the owner is unknown and optimistic mode is disabled. */
    public static final String REASON_UNKNOWN_STRICT = "owner unknown, strict mode";

    /** Reason text used when the entity is a player. */
    public static final String REASON_PLAYER_ENTITY = "player entities are always node-local";

    /** Reason text used when the local node owns the resource. */
    public static final String REASON_LOCAL_OWNER = "local ownership";

    /** Reason text used when a foreign node owns the resource. */
    public static final String REASON_FOREIGN_OWNER = "owned by another node";

    private OwnershipPolicy() {
        // Utility class.
    }

    /**
     * Decides whether a block mutation may execute on this node.
     *
     * @param serverSide                 whether the level is a {@code ServerLevel}; client levels must
     *                                   never be gated (they have no ownership data)
     * @param clusterActive              whether a cluster session is established and authoritative
     * @param chunkLoaded                result of {@code Level.isLoaded(pos)} for the target position
     * @param localNode                  id of this node, {@code null} only before the bootstrap finished
     * @param knownOwner                 owner reported by {@link ChunkOwnershipRegistry}, or {@code null}
     *                                   when the master has no assignment for the chunk
     * @param assumeOwnershipWhenUnknown whether an unassigned chunk is treated as local (optimistic)
     *                                   or rejected (strict). Optimistic keeps single-node-like mods
     *                                   working while the ownership snapshot is still in flight.
     * @return the verdict, never {@code null}
     * @implNote The method is allocation-light (one record) because it runs for every
     *         {@code Level.setBlock} call on the server thread.
     */
    public static MutationVerdict evaluateBlockMutation(boolean serverSide,
                                                        boolean clusterActive,
                                                        boolean chunkLoaded,
                                                        ClusterNodeId localNode,
                                                        ClusterNodeId knownOwner,
                                                        boolean assumeOwnershipWhenUnknown) {
        if (!serverSide) {
            return MutationVerdict.allow(REASON_CLIENT_SIDE);
        }
        if (!clusterActive || localNode == null) {
            return MutationVerdict.allow(REASON_STANDALONE);
        }
        if (knownOwner != null && !knownOwner.isSameNode(localNode)) {
            return MutationVerdict.relay(REASON_FOREIGN_OWNER + " (" + knownOwner + ")");
        }
        if (!chunkLoaded) {
            return MutationVerdict.reject(REASON_CHUNK_UNLOADED);
        }
        if (knownOwner == null) {
            return assumeOwnershipWhenUnknown
                    ? MutationVerdict.allow(REASON_UNKNOWN_OPTIMISTIC)
                    : MutationVerdict.reject(REASON_UNKNOWN_STRICT);
        }
        return MutationVerdict.allow(REASON_LOCAL_OWNER);
    }

    /**
     * Decides whether an entity may be added to / ticked by this node.
     *
     * @param serverSide                 whether the level is a {@code ServerLevel}
     * @param clusterActive              whether a cluster session is authoritative
     * @param entityIsPlayer             whether the entity is a {@code ServerPlayer}. Players are
     *                                   inherently local: they are connected to exactly one node, and
     *                                   relaying a player entity would duplicate it.
     * @param localNode                  id of this node, may be {@code null} before bootstrap
     * @param knownOwner                 entity/chunk owner reported by the registry, or {@code null}
     * @param assumeOwnershipWhenUnknown optimistic vs strict handling of unassigned entities
     * @return the verdict, never {@code null}
     * @implNote Unlike block mutations there is no "loaded" precondition: entity ownership is
     *         tracked by the master per entity id when available, and otherwise inherited from the
     *         chunk that contains the entity.
     */
    public static MutationVerdict evaluateEntityMutation(boolean serverSide,
                                                         boolean clusterActive,
                                                         boolean entityIsPlayer,
                                                         ClusterNodeId localNode,
                                                         ClusterNodeId knownOwner,
                                                         boolean assumeOwnershipWhenUnknown) {
        if (!serverSide) {
            return MutationVerdict.allow(REASON_CLIENT_SIDE);
        }
        if (!clusterActive || localNode == null) {
            return MutationVerdict.allow(REASON_STANDALONE);
        }
        if (entityIsPlayer) {
            return MutationVerdict.allow(REASON_PLAYER_ENTITY);
        }
        if (knownOwner != null && !knownOwner.isSameNode(localNode)) {
            return MutationVerdict.relay(REASON_FOREIGN_OWNER + " (" + knownOwner + ")");
        }
        if (knownOwner == null) {
            return assumeOwnershipWhenUnknown
                    ? MutationVerdict.allow(REASON_UNKNOWN_OPTIMISTIC)
                    : MutationVerdict.reject(REASON_UNKNOWN_STRICT);
        }
        return MutationVerdict.allow(REASON_LOCAL_OWNER);
    }

    /**
     * Verifies that an inbound relayed mutation really belongs to this node before applying it.
     *
     * <h2>Why this exists</h2>
     * A relayed mutation must be applied exactly once. If ownership changed while the frame was in
     * flight, applying it here would resurrect a block change on a node that no longer owns the
     * chunk - the classic "ghost block" of distributed world storage. The dispatcher therefore
     * re-checks ownership on arrival and drops the frame when the master has reassigned the chunk.
     *
     * @param clusterActive whether a cluster session is authoritative
     * @param localNode     id of this node, may be {@code null}
     * @param targetOwner   node the frame was addressed to, may be {@code null} for broadcasts
     * @return ALLOW when this node may apply the frame, REJECT otherwise
     */
    public static MutationVerdict evaluateRelayDelivery(boolean clusterActive,
                                                        ClusterNodeId localNode,
                                                        ClusterNodeId targetOwner) {
        if (!clusterActive || localNode == null) {
            return MutationVerdict.allow(REASON_STANDALONE);
        }
        if (targetOwner == null) {
            return MutationVerdict.allow("broadcast frame");
        }
        if (targetOwner.isSameNode(localNode)) {
            return MutationVerdict.allow(REASON_LOCAL_OWNER);
        }
        return MutationVerdict.reject("addressed to " + targetOwner + ", not this node");
    }
}