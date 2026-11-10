package dev.forgetalker.event;

import dev.forgetalker.ForgeTalker;
import dev.forgetalker.ForgeTalkerRuntime;
import dev.forgetalker.bridge.message.MutationMessages.ClusterEvent;
import dev.forgetalker.bridge.message.MutationMessages.PlayerPresence;
import dev.forgetalker.bridge.message.MutationMessages.PresenceKind;
import dev.forgetalker.bridge.message.OwnershipMessages.InterestKind;
import dev.forgetalker.cluster.ChunkCoordinate;
import dev.forgetalker.cluster.MutationVerdict;
import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.Blocks;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraftforge.event.TickEvent;
import net.minecraftforge.event.entity.EntityJoinLevelEvent;
import net.minecraftforge.event.entity.player.PlayerEvent;
import net.minecraftforge.event.level.BlockEvent;
import net.minecraftforge.event.level.ChunkEvent;
import net.minecraftforge.event.server.ServerStartedEvent;
import net.minecraftforge.event.server.ServerStoppingEvent;
import net.minecraftforge.eventbus.api.SubscribeEvent;
import net.minecraftforge.fml.common.Mod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Forge bus subscriber that reacts to server lifecycle and world events.
 *
 * <h2>Operational purpose</h2>
 * Turns Forge events into cluster traffic and enforces ownership at the event level, complementing
 * the bytecode guards:
 * <ul>
 *   <li>{@code ServerStarted/Stopping} - lifecycle of {@link ForgeTalkerRuntime}</li>
 *   <li>{@code PlayerLoggedIn/Out} - player presence broadcast (who is on which node)</li>
 *   <li>{@code BlockEvent.BreakEvent} - cancel + relay breaks in foreign chunks, broadcast local breaks</li>
 *   <li>{@code EntityJoinLevelEvent} - cancel + relay spawns in foreign chunks</li>
 *   <li>{@code ChunkEvent.Load/Unload} - ownership interest hints for the master</li>
 *   <li>{@code ServerTickEvent} - drains the deferred mutation queue</li>
 * </ul>
 *
 * <h2>Thread safety</h2>
 * All handlers run on the server thread. They may read the runtime and enqueue frames; they never
 * block on network I/O. {@code ChunkEvent} fires extremely often, so its handler is deliberately
 * allocation-light and respects the {@code announceChunkInterest} switch.
 *
 * <h2>Why events in addition to mixins</h2>
 * Mixins catch the low-level write paths (including mods that bypass events), while events carry
 * semantic information (which player broke the block) that the cluster event bus needs. Keeping both
 * is intentional; see {@code docs/ARCHITECTURE.md}.
 */
@Mod.EventBusSubscriber(modid = ForgeTalker.MOD_ID, bus = Mod.EventBusSubscriber.Bus.FORGE)
public final class ForgeTalkerEventBridge {

    private static final Logger LOGGER = LogManager.getLogger("ForgeTalker/Events");

    private ForgeTalkerEventBridge() {
        // Static event subscriber.
    }

    /**
     * Bootstraps the runtime once the server is fully started.
     *
     * @param event Forge lifecycle event, never {@code null}
     * @implNote The runtime is not built earlier because {@code MinecraftServer.getLevel} and the
     *         player list are only reliable at this point.
     */
    @SubscribeEvent
    public static void onServerStarted(ServerStartedEvent event) {
        ForgeTalkerRuntime.bootstrap(event.getServer());
    }

    /**
     * Shuts the runtime down and tells the master why.
     *
     * @param event Forge lifecycle event, never {@code null}
     */
    @SubscribeEvent
    public static void onServerStopping(ServerStoppingEvent event) {
        ForgeTalkerRuntime.shutdown("server stopping");
    }

    /**
     * Broadcasts a player join so other nodes can mirror cross-node player state.
     *
     * @param event login event, never {@code null}
     */
    @SubscribeEvent
    public static void onPlayerLoggedIn(PlayerEvent.PlayerLoggedInEvent event) {
        Player player = event.getEntity();
        if (!(player instanceof ServerPlayer serverPlayer)) {
            return;
        }
        ForgeTalkerRuntime runtime = ForgeTalkerRuntime.get();
        if (runtime == null) {
            return;
        }
        runtime.bridge().broadcastPresence(new PlayerPresence(serverPlayer.getUUID(),
                serverPlayer.getGameProfile().getName(), PresenceKind.JOIN,
                runtime.nodeId().value()));
        ServerLevel level = serverPlayer.serverLevel();
        runtime.bridge().announceInterest(
                ChunkCoordinate.fromBlock(level.dimension().location().toString(),
                        serverPlayer.blockPosition().getX(), serverPlayer.blockPosition().getZ()),
                InterestKind.CLAIM);
        LOGGER.debug("Player {} joined on node {}", serverPlayer.getGameProfile().getName(),
                runtime.nodeId());
    }

    /**
     * Broadcasts a player leave.
     *
     * @param event logout event, never {@code null}
     */
    @SubscribeEvent
    public static void onPlayerLoggedOut(PlayerEvent.PlayerLoggedOutEvent event) {
        Player player = event.getEntity();
        if (!(player instanceof ServerPlayer serverPlayer)) {
            return;
        }
        ForgeTalkerRuntime runtime = ForgeTalkerRuntime.get();
        if (runtime == null) {
            return;
        }
        runtime.bridge().broadcastPresence(new PlayerPresence(serverPlayer.getUUID(),
                serverPlayer.getGameProfile().getName(), PresenceKind.LEAVE,
                runtime.nodeId().value()));
    }

    /**
     * Enforces ownership for block breaks and broadcasts local breaks.
     *
     * <h2>Behaviour</h2>
     * <ul>
     *   <li>Foreign chunk: the break is cancelled and relayed to the owner as an AIR block change.</li>
     *   <li>Unresolvable chunk: cancelled (the player will simply have to retry once ownership settles).</li>
     *   <li>Local chunk: the break proceeds and a {@code block_break} cluster event is broadcast.</li>
     * </ul>
     *
     * @param event break event, never {@code null}
     */
    @SubscribeEvent
    public static void onBlockBreak(BlockEvent.BreakEvent event) {
        if (!(event.getLevel() instanceof ServerLevel level)) {
            return;
        }
        ForgeTalkerRuntime runtime = ForgeTalkerRuntime.get();
        if (runtime == null) {
            return;
        }
        BlockPos pos = event.getPos();
        MutationVerdict verdict = ForgeTalkerRuntime.evaluateBlockMutation(level, pos);
        if (!verdict.isAllowed()) {
            if (verdict.isRelayed()) {
                BlockState air = Blocks.AIR.defaultBlockState();
                ForgeTalkerRuntime.relayBlockMutation(level, pos, air, 3);
            }
            if (runtime.effectiveSettings().debugLogging()) {
                LOGGER.debug("Cancelled block break at {}: {}", pos, verdict);
            }
            event.setCanceled(true);
            return;
        }
        Player player = event.getPlayer();
        runtime.bridge().broadcastEvent(new ClusterEvent(
                "block_break",
                level.dimension().location().toString(),
                pos.getX(), pos.getY(), pos.getZ(),
                player == null ? null : player.getUUID(),
                event.getState().getBlock().toString(),
                System.currentTimeMillis()));
    }

    /**
     * Enforces ownership for entity spawns that happen through the event path.
     *
     * @param event entity join event, never {@code null}
     * @implNote Entities loaded from disk are skipped: they already exist on their owning node and
     *         relaying them would duplicate the entity on every node that loads the chunk.
     */
    @SubscribeEvent
    public static void onEntityJoinLevel(EntityJoinLevelEvent event) {
        if (event.loadedFromDisk()) {
            return;
        }
        Entity entity = event.getEntity();
        Level level = event.getLevel();
        if (!(level instanceof ServerLevel serverLevel)) {
            return;
        }
        ForgeTalkerRuntime runtime = ForgeTalkerRuntime.get();
        if (runtime == null) {
            return;
        }
        MutationVerdict verdict = ForgeTalkerRuntime.evaluateEntityMutation(entity);
        if (!verdict.isAllowed()) {
            if (verdict.isRelayed()) {
                ForgeTalkerRuntime.relayEntitySpawn(serverLevel, entity);
            }
            event.setCanceled(true);
            return;
        }
        runtime.bridge().broadcastEvent(new ClusterEvent(
                "entity_join",
                serverLevel.dimension().location().toString(),
                entity.blockPosition().getX(), entity.blockPosition().getY(),
                entity.blockPosition().getZ(), null,
                entity.getType().toString(),
                System.currentTimeMillis()));
    }

    /**
     * Sends an ownership CLAIM hint when a chunk becomes loaded.
     *
     * @param event chunk load event, never {@code null}
     */
    @SubscribeEvent
    public static void onChunkLoad(ChunkEvent.Load event) {
        if (!(event.getLevel() instanceof ServerLevel level) || !(event.getChunk() instanceof LevelChunk chunk)) {
            return;
        }
        ForgeTalkerRuntime runtime = ForgeTalkerRuntime.get();
        if (runtime == null) {
            return;
        }
        runtime.bridge().announceInterest(
                ChunkCoordinate.of(level.dimension().location().toString(), chunk.getPos().x, chunk.getPos().z),
                InterestKind.CLAIM);
    }

    /**
     * Sends an ownership RELEASE hint when a chunk is unloaded.
     *
     * @param event chunk unload event, never {@code null}
     */
    @SubscribeEvent
    public static void onChunkUnload(ChunkEvent.Unload event) {
        if (!(event.getLevel() instanceof ServerLevel level) || !(event.getChunk() instanceof LevelChunk chunk)) {
            return;
        }
        ForgeTalkerRuntime runtime = ForgeTalkerRuntime.get();
        if (runtime == null) {
            return;
        }
        runtime.bridge().announceInterest(
                ChunkCoordinate.of(level.dimension().location().toString(), chunk.getPos().x, chunk.getPos().z),
                InterestKind.RELEASE);
    }

    /**
     * Drains deferred mutations at the end of every server tick.
     *
     * @param event server tick event, never {@code null}
     */
    @SubscribeEvent
    public static void onServerTick(TickEvent.ServerTickEvent event) {
        if (event.phase == TickEvent.Phase.END) {
            ForgeTalkerRuntime.drainDeferred();
        }
    }
}
