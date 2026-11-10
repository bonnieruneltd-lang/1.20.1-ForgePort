package dev.forgetalker.dispatch;

import dev.forgetalker.bridge.message.MutationMessages.PayloadRelay;
import io.netty.buffer.Unpooled;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.network.protocol.game.ServerboundCustomPayloadPacket;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import net.minecraftforge.network.NetworkHooks;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Re-delivers a payload that was observed on another node to the local players.
 *
 * <h2>Operational purpose</h2>
 * Forge mods register their packets through {@code SimpleChannel} and Forge only invokes those
 * handlers when a packet arrives on a player's connection. On a MultiPaper cluster the player is
 * connected to exactly one node, so a payload that must be processed cluster-wide would otherwise
 * reach only that node. This class reconstructs a {@link ServerboundCustomPayloadPacket} and feeds
 * it back into Forge's own dispatch path ({@link NetworkHooks#onCustomPayload}), so mods receive a
 * normal {@code NetworkEvent} without any mod-specific glue code.
 *
 * <h2>Semantic caveat (documented on purpose)</h2>
 * The reconstructed packet is delivered through a <em>local</em> player's connection, so a mod that
 * inspects {@code ctx.getSender()} sees that local player rather than the original sender. This is
 * the same compromise a proxy makes; mods that key on the sender are listed in
 * {@code docs/ARCHITECTURE.md} as "payload relay unsafe" and should be excluded by channel name.
 *
 * <h2>Thread safety</h2>
 * MUST be called on the server thread: Forge network handlers are allowed to touch world state.
 * The method itself is stateless.
 */
public final class RelayedPayloadDispatcher {

    private static final Logger LOGGER = LogManager.getLogger("ForgeTalker/PayloadRelay");

    /**
     * Delivers the payload to every player connected to this node.
     *
     * @param server  running server, never {@code null}
     * @param relay   relayed payload frame, never {@code null}
     * @param hopLimit configured maximum hop count
     * @return the number of players the payload was offered to
     */
    public int deliver(MinecraftServer server, PayloadRelay relay, int hopLimit) {
        if (!relay.canForward(hopLimit)) {
            LOGGER.debug("Dropping payload on {}: hop {} exceeds limit {}",
                    relay.channel(), relay.hop(), hopLimit);
            return 0;
        }
        ResourceLocation channel = ResourceLocation.tryParse(relay.channel());
        if (channel == null) {
            LOGGER.warn("Dropping payload with invalid channel name '{}'", relay.channel());
            return 0;
        }
        byte[] payload = relay.payload();
        List<ServerPlayer> players = server.getPlayerList().getPlayers();
        int delivered = 0;
        for (ServerPlayer player : players) {
            // A fresh buffer per player: a mod handler may consume/advance the reader index, and
            // sharing one buffer would make the second player see an empty payload.
            FriendlyByteBuf buffer = new FriendlyByteBuf(Unpooled.wrappedBuffer(payload.clone()));
            ServerboundCustomPayloadPacket packet = new ServerboundCustomPayloadPacket(channel, buffer);
            try {
                NetworkHooks.onCustomPayload(packet, player.connection.connection);
                delivered++;
            } catch (RuntimeException e) {
                LOGGER.warn("Mod payload handler for {} failed while replaying on {}: {}",
                        channel, player.getGameProfile().getName(), e.toString());
            }
        }
        if (delivered == 0) {
            LOGGER.debug("No local players to deliver payload {} to", channel);
        }
        return delivered;
    }
}
