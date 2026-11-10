package dev.forgetalker.cluster;

import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.level.Level;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Minecraft-aware facade over {@link OwnershipPolicy}.
 *
 * <h2>Operational purpose</h2>
 * Turns a {@code Level}/{@code BlockPos}/{@code Entity} into the plain inputs the pure policy needs
 * (dimension string, chunk coordinates, loaded flag, owner lookup) and applies the configured
 * optimistic/strict mode. Every interception point - mixins, event handlers, the relay dispatcher -
 * funnels through this class, so there is exactly one definition of "local" in the codebase.
 *
 * <h2>Remote-apply scope</h2>
 * When this node applies a mutation that was relayed from a peer, the mutation re-enters
 * {@code Level.setBlock} and would be evaluated again. {@link #enterRemoteApply()} sets a
 * thread-local flag that makes {@link #evaluateBlockMutation(Level, BlockPos)} and
 * {@link #evaluateEntityMutation(Entity)} return ALLOW for the duration of the apply. This is
 * necessary because the mutation must be applied even if the local registry mirror has not yet seen
 * the ownership delta that made this node the owner.
 *
 * <h2>Thread safety</h2>
 * All fields are immutable or thread-safe suppliers. The remote-apply scope is a {@link ThreadLocal},
 * which is correct because mutations are only applied on the server thread. The guard itself may be
 * queried from the Netty event loop (payload relay) without acquiring a lock.
 *
 * <h2>Cluster context</h2>
 * Queries are answered from {@link ChunkOwnershipRegistry}, the locally mirrored copy of the
 * master's table. No network call is ever made from a guard.
 */
public final class ChunkOwnershipGuard {

    private final ChunkOwnershipRegistry registry;
    private final Supplier<ClusterNodeId> localNode;
    private final BooleanSupplier clusterActive;
    private final BooleanSupplier assumeOwnershipWhenUnknown;
    private final ThreadLocal<Boolean> remoteApplyScope = ThreadLocal.withInitial(() -> Boolean.FALSE);

    /**
     * @param registry                     mirror of the master's ownership table, never {@code null}
     * @param localNode                    supplier of this node's id, may return {@code null} before
     *                                     the session is established
     * @param clusterActive                whether the session is authoritative
     * @param assumeOwnershipWhenUnknown   optimistic vs strict handling of unassigned chunks
     */
    public ChunkOwnershipGuard(ChunkOwnershipRegistry registry,
                               Supplier<ClusterNodeId> localNode,
                               BooleanSupplier clusterActive,
                               BooleanSupplier assumeOwnershipWhenUnknown) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.localNode = Objects.requireNonNull(localNode, "localNode");
        this.clusterActive = Objects.requireNonNull(clusterActive, "clusterActive");
        this.assumeOwnershipWhenUnknown = Objects.requireNonNull(assumeOwnershipWhenUnknown,
                "assumeOwnershipWhenUnknown");
    }

    /**
     * @return the registry this guard reads from, never {@code null}
     */
    public ChunkOwnershipRegistry registry() {
        return registry;
    }

    /**
     * Spec-mandated helper: is this position inside a chunk owned by this node?
     *
     * @param level server level, never {@code null}
     * @param pos   block position, never {@code null}
     * @return {@code true} when the local node may mutate the block
     * @implNote Equivalent to {@link #evaluateBlockMutation(Level, BlockPos)} returning ALLOW, but
     *         without allocating a verdict for callers that only need a boolean.
     */
    public boolean isLocalChunk(ServerLevel level, BlockPos pos) {
        return evaluateBlockMutation(level, pos).isAllowed();
    }

    /**
     * Spec-mandated helper: does this entity belong to this node?
     *
     * @param entity entity to test, may be {@code null}
     * @return {@code true} when the entity may be spawned/ticked here; {@code false} for null
     */
    public boolean isLocalEntity(Entity entity) {
        return evaluateEntityMutation(entity).isAllowed();
    }

    /**
     * Evaluates a block mutation.
     *
     * @param level level the mutation targets, never {@code null}
     * @param pos   mutated position, never {@code null}
     * @return the verdict, never {@code null}
     */
    public MutationVerdict evaluateBlockMutation(Level level, BlockPos pos) {
        Objects.requireNonNull(level, "level");
        Objects.requireNonNull(pos, "pos");
        if (remoteApplyScope.get()) {
            return MutationVerdict.allow("remote apply scope");
        }
        boolean serverSide = level instanceof ServerLevel;
        boolean active = serverSide && clusterActive.getAsBoolean();
        ClusterNodeId node = localNode.get();
        ChunkCoordinate chunk = chunkOf(level, pos);
        ClusterNodeId owner = active ? registry.ownerOf(chunk) : null;
        return OwnershipPolicy.evaluateBlockMutation(serverSide, active, level.isLoaded(pos), node, owner,
                assumeOwnershipWhenUnknown.getAsBoolean());
    }

    /**
     * Evaluates whether an entity may be added to this level.
     *
     * @param entity entity to evaluate, may be {@code null}
     * @return the verdict, never {@code null}; REJECT for {@code null}
     */
    public MutationVerdict evaluateEntityMutation(Entity entity) {
        if (entity == null) {
            return MutationVerdict.reject("null entity");
        }
        if (remoteApplyScope.get()) {
            return MutationVerdict.allow("remote apply scope");
        }
        boolean serverSide = entity.level() instanceof ServerLevel;
        boolean active = serverSide && clusterActive.getAsBoolean();
        if (!active) {
            return OwnershipPolicy.evaluateEntityMutation(serverSide, false, entity instanceof Player,
                    localNode.get(), null, true);
        }
        ClusterNodeId owner = registry.ownerOf(chunkOf(entity.level(), entity.blockPosition()));
        return OwnershipPolicy.evaluateEntityMutation(true, true, entity instanceof Player,
                localNode.get(), owner, assumeOwnershipWhenUnknown.getAsBoolean());
    }

    /**
     * Builds the registry key for a position.
     *
     * @param level level providing the dimension id, never {@code null}
     * @param pos   block position, never {@code null}
     * @return the chunk coordinate, never {@code null}
     */
    public ChunkCoordinate chunkOf(Level level, BlockPos pos) {
        return ChunkCoordinate.fromBlock(level.dimension().location().toString(), pos.getX(), pos.getZ());
    }

    /**
     * Marks the current thread as applying a relayed mutation.
     *
     * <p>Always pair with {@link #exitRemoteApply()} in a {@code finally} block:
     * <pre>{@code
     * guard.enterRemoteApply();
     * try { level.setBlock(pos, state, flags); } finally { guard.exitRemoteApply(); }
     * }</pre>
     */
    public void enterRemoteApply() {
        remoteApplyScope.set(Boolean.TRUE);
    }

    /**
     * Clears the remote-apply marker for the current thread.
     */
    public void exitRemoteApply() {
        remoteApplyScope.remove();
    }

    /**
     * @return {@code true} when the calling thread is inside a relayed apply scope
     */
    public boolean isInRemoteApplyScope() {
        return remoteApplyScope.get();
    }
}
