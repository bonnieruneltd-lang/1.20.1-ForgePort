package dev.forgetalker.mixin;

import dev.forgetalker.ForgeTalkerRuntime;
import dev.forgetalker.cluster.MutationVerdict;
import dev.forgetalker.util.MixinProbe;
import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.state.BlockState;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/**
 * Ownership guard for {@code Level.setBlock(BlockPos, BlockState, int, int)}.
 *
 * <h2>Why this injection point</h2>
 * In 1.20.1 every block write funnels through the four-argument {@code setBlock}: the three-argument
 * overload delegates to it, {@code setBlockAndUpdate} delegates to it, {@code destroyBlock} delegates
 * to it, and Forge's own {@code ForgeHooks.onPlaceItemIntoWorld} path ends there too. Guarding the
 * three-argument overload instead would let {@code setBlock(pos, state, flags, recursionLeft)} calls
 * from mods bypass the check. Injecting at {@code HEAD} (not {@code RETURN}) is required because the
 * guard must prevent the write entirely, not observe it.
 *
 * <h2>Why {@code Level} and not {@code ServerLevel}</h2>
 * {@code ServerLevel} does not override {@code setBlock}; the method lives on {@code Level}. A mixin
 * on {@code ServerLevel} would never fire. The handler casts {@code this} back to {@code Level} and
 * only enforces when the instance is a {@code ServerLevel}.
 *
 * <h2>Thread safety</h2>
 * Runs on the server thread (or a worldgen worker for proto-chunks, where the guard returns ALLOW
 * because the level is not a {@code ServerLevel}). Delegates to {@link ForgeTalkerRuntime}, which is
 * null-safe and never throws into {@code setBlock}.
 *
 * <h2>Cluster context</h2>
 * Foreign chunk: the write is cancelled and re-sent to the owning node as a {@code BLOCK_MUTATION}
 * frame with the same state id and flags. Unknown owner / unloaded chunk: cancelled, matching
 * vanilla's behaviour of discarding writes into non-loaded chunks.
 */
@Mixin(Level.class)
public abstract class LevelSetBlockMixin {

    static {
        // Merged into Level.<clinit>; proves the mixin applied (see /forgetalker status).
        MixinProbe.markApplied("Level.setBlock");
    }

    /**
     * Checks ownership before a block state is written.
     *
     * @param pos           mutated block position, never {@code null}
     * @param state         requested block state, never {@code null}
     * @param flags         vanilla {@code Block.UPDATE_*} flags
     * @param recursionLeft recursion budget of the vanilla neighbour update cascade
     * @param cir           cancellable return handle; set to {@code false} to abort the write
     * @implNote The descriptor is written in named (official) mappings and remapped to SRG by the
     *         Mixin annotation processor. Do not "simplify" it to just the method name: {@code Level}
     *         has two {@code setBlock} overloads and Mixin would refuse an ambiguous target.
     */
    @Inject(
            method = "setBlock(Lnet/minecraft/core/BlockPos;Lnet/minecraft/world/level/block/state/BlockState;II)Z",
            at = @At("HEAD"),
            cancellable = true)
    private void forgetalker$guardSetBlock(BlockPos pos, BlockState state, int flags, int recursionLeft,
                                           CallbackInfoReturnable<Boolean> cir) {
        Level level = (Level) (Object) this;
        if (!(level instanceof ServerLevel serverLevel)) {
            return;
        }
        MutationVerdict verdict = ForgeTalkerRuntime.evaluateBlockMutation(serverLevel, pos);
        if (verdict.isAllowed()) {
            return;
        }
        if (verdict.isRelayed()) {
            ForgeTalkerRuntime.relayBlockMutation(serverLevel, pos, state, flags);
        }
        cir.setReturnValue(Boolean.FALSE);
    }
}
