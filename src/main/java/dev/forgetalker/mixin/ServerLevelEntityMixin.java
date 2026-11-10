package dev.forgetalker.mixin;

import dev.forgetalker.ForgeTalkerRuntime;
import dev.forgetalker.cluster.MutationVerdict;
import dev.forgetalker.util.MixinProbe;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.Entity;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/**
 * Ownership guard for {@code ServerLevel.addFreshEntity(Entity)}.
 *
 * <h2>Why this injection point</h2>
 * In 1.20.1 {@code addFreshEntity} is the single entry point used by mob spawning, item drops, modded
 * spawners and {@code addFreshEntityWithPassengers}. It sits on {@code ServerLevel} (not on
 * {@code Level}), which is convenient: the guard never has to filter client levels.
 *
 * <h2>Why cancel instead of relocating</h2>
 * Spawning the entity locally and deleting it afterwards would fire {@code EntityJoinLevelEvent} and
 * all mod hooks twice. Cancelling at {@code HEAD} keeps observers consistent: the entity simply never
 * joins this node, and the owning node spawns it once from the relayed {@code ENTITY_SPAWN} frame.
 *
 * <h2>Thread safety</h2>
 * Server thread only. {@link ForgeTalkerRuntime} is null-safe and swallows failures so a guard bug
 * cannot crash the tick loop.
 *
 * <h2>Cluster context</h2>
 * The entity's chunk is resolved through {@code ServerLevel} and the dimension key. Players are
 * always treated as local (a player is connected to exactly one node), so the guard can never cancel
 * a player join.
 */
@Mixin(ServerLevel.class)
public abstract class ServerLevelEntityMixin {

    static {
        MixinProbe.markApplied("ServerLevel.addFreshEntity");
    }

    /**
     * Blocks entity spawns for chunks this node does not own.
     *
     * @param entity entity about to be added, never {@code null}
     * @param cir    cancellable return handle; set to {@code false} to abort the spawn
     */
    @Inject(method = "addFreshEntity", at = @At("HEAD"), cancellable = true)
    private void forgetalker$guardAddFreshEntity(Entity entity, CallbackInfoReturnable<Boolean> cir) {
        ServerLevel level = (ServerLevel) (Object) this;
        MutationVerdict verdict = ForgeTalkerRuntime.evaluateEntityMutation(entity);
        if (verdict.isAllowed()) {
            return;
        }
        if (verdict.isRelayed()) {
            ForgeTalkerRuntime.relayEntitySpawn(level, entity);
        }
        cir.setReturnValue(Boolean.FALSE);
    }
}
