package dev.forgetalker.command;

import com.mojang.brigadier.CommandDispatcher;
import com.mojang.brigadier.context.CommandContext;
import dev.forgetalker.ForgeTalker;
import dev.forgetalker.ForgeTalkerRuntime;
import dev.forgetalker.bridge.message.ControlMessages.StatusResponse;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.network.chat.Component;
import net.minecraftforge.event.RegisterCommandsEvent;
import net.minecraftforge.eventbus.api.SubscribeEvent;
import net.minecraftforge.fml.common.Mod;

import java.util.Map;

/**
 * Operator command {@code /forgetalker} used to inspect the cluster from inside the game or console.
 *
 * <h2>Operational purpose</h2>
 * Gives an operator - or an automated verification run - a first-class way to see the node id, the
 * transport state, the number of owned chunks, deferred mutations, metrics and, crucially, which
 * mixins were applied. Without it, diagnosing a silent mixin failure requires log archaeology.
 *
 * <h2>Thread safety</h2>
 * Command execution happens on the server thread. Status reads are lock-free snapshots. Peer status
 * requests are fire-and-forget; their replies arrive asynchronously and are visible on the next
 * invocation.
 *
 * <h2>Permissions</h2>
 * Requires permission level 2, matching other administrative server commands.
 */
@Mod.EventBusSubscriber(modid = ForgeTalker.MOD_ID, bus = Mod.EventBusSubscriber.Bus.FORGE)
public final class ForgeTalkerCommand {

    private ForgeTalkerCommand() {
        // Static event subscriber.
    }

    /**
     * Registers {@code /forgetalker status|ownership|peers}.
     *
     * @param event Forge command registration event, never {@code null}
     */
    @SubscribeEvent
    public static void onRegisterCommands(RegisterCommandsEvent event) {
        CommandDispatcher<CommandSourceStack> dispatcher = event.getDispatcher();
        dispatcher.register(Commands.literal("forgetalker")
                .requires(source -> source.hasPermission(2))
                .then(Commands.literal("status").executes(ForgeTalkerCommand::status))
                .then(Commands.literal("ownership").executes(ForgeTalkerCommand::ownership))
                .then(Commands.literal("peers").executes(ForgeTalkerCommand::peers)));
    }

    /**
     * Prints the full status report and asks every peer for a fresh snapshot.
     *
     * @param context command context, never {@code null}
     * @return {@code 1} so command blocks can detect success
     */
    private static int status(CommandContext<CommandSourceStack> context) {
        for (String line : ForgeTalkerRuntime.statusLines()) {
            context.getSource().sendSuccess(() -> Component.literal(line), false);
        }
        ForgeTalkerRuntime runtime = ForgeTalkerRuntime.get();
        if (runtime != null) {
            runtime.dispatcher().requestStatusFromAllPeers();
        }
        return 1;
    }

    /**
     * Prints the ownership table of this node.
     *
     * @param context command context, never {@code null}
     * @return the number of owned chunks reported, or {@code 0} when no runtime is active
     */
    private static int ownership(CommandContext<CommandSourceStack> context) {
        ForgeTalkerRuntime runtime = ForgeTalkerRuntime.get();
        if (runtime == null) {
            context.getSource().sendSuccess(() -> Component.literal(
                    "Forge Talker runtime is not active."), false);
            return 0;
        }
        Map<?, ?> owned = runtime.registry().chunksOwnedBy(runtime.nodeId());
        context.getSource().sendSuccess(() -> Component.literal(
                "Node " + runtime.nodeId() + " owns " + owned.size() + " chunk(s) at epoch "
                        + runtime.registry().currentEpoch() + " (known cluster-wide: "
                        + runtime.registry().size() + ")"), false);
        int shown = 0;
        for (Object chunk : owned.keySet()) {
            if (shown++ >= 20) {
                context.getSource().sendSuccess(() -> Component.literal("... more entries suppressed"), false);
                break;
            }
            context.getSource().sendSuccess(() -> Component.literal("  " + chunk), false);
        }
        return owned.size();
    }

    /**
     * Prints the last known status of each peer.
     *
     * @param context command context, never {@code null}
     * @return the number of known peer snapshots
     */
    private static int peers(CommandContext<CommandSourceStack> context) {
        ForgeTalkerRuntime runtime = ForgeTalkerRuntime.get();
        if (runtime == null) {
            context.getSource().sendSuccess(() -> Component.literal(
                    "Forge Talker runtime is not active."), false);
            return 0;
        }
        runtime.dispatcher().requestStatusFromAllPeers();
        Map<String, StatusResponse> statuses = runtime.dispatcher().peerStatuses();
        context.getSource().sendSuccess(() -> Component.literal(
                "Live peer links: " + runtime.bridge().transport().peerCount()
                        + ", known snapshots: " + statuses.size()), false);
        for (StatusResponse response : statuses.values()) {
            context.getSource().sendSuccess(() -> Component.literal(
                    "  " + response.nodeId() + " [" + response.state() + "] chunks="
                            + response.ownedChunks() + " peers=" + response.peerCount()
                            + " epoch=" + response.ownershipEpoch()), false);
        }
        return statuses.size();
    }
}
