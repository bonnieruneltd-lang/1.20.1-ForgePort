package dev.forgetalker.network;

import dev.forgetalker.ForgeTalker;
import dev.forgetalker.ForgeTalkerRuntime;
import dev.forgetalker.network.packet.ClusterStatusRequestPacket;
import dev.forgetalker.network.packet.ClusterStatusResponsePacket;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerPlayer;
import net.minecraftforge.network.NetworkDirection;
import net.minecraftforge.network.NetworkRegistry;
import net.minecraftforge.network.PacketDistributor;
import net.minecraftforge.network.simple.SimpleChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Forge {@code SimpleChannel} for Forge Talker's own client/server traffic.
 *
 * <h2>Operational purpose</h2>
 * The cluster bus (master + peers) is a raw Netty implementation because MultiPaper's protocol is
 * not a Minecraft packet protocol. This channel is different: it is the standard Forge path for
 * traffic between a player's client and the node it is connected to. It carries status queries used
 * by the client UI and by integration tests.
 *
 * <p>Modded payloads that belong to <em>other</em> mods never travel on this channel. They are
 * observed by {@code ModdedPayloadRelay} and forwarded over the cluster bus, because a relayed
 * payload must reach nodes the player is not connected to.
 *
 * <h2>Thread safety</h2>
 * {@link SimpleChannel} is thread safe for sending. Registration must happen once during mod
 * construction on the mod-loading thread; handlers use {@code consumerMainThread} so the payload
 * bodies run on the server/client main thread.
 *
 * <h2>Versioning</h2>
 * The protocol version is a constant string. Forge refuses to connect a client and server whose
 * versions differ for this channel, which prevents desynchronised status packets after an upgrade.
 */
public final class ForgeTalkerNetwork {

    private static final Logger LOGGER = LogManager.getLogger("ForgeTalker/Network");

    /** Protocol version of the Forge Talker status channel. */
    public static final String PROTOCOL_VERSION = "1";

    /** Channel name, namespaced with the mod id. */
    public static final ResourceLocation CHANNEL_NAME =
            new ResourceLocation(ForgeTalker.MOD_ID, "main");

    private static final SimpleChannel CHANNEL = NetworkRegistry.newSimpleChannel(
            CHANNEL_NAME,
            () -> PROTOCOL_VERSION,
            PROTOCOL_VERSION::equals,
            PROTOCOL_VERSION::equals);

    private ForgeTalkerNetwork() {
        // Holder class.
    }

    /**
     * Registers the packets. Must be called exactly once from the mod constructor.
     *
     * @throws IllegalStateException if Forge rejects a duplicate registration
     */
    public static void register() {
        int index = 0;
        CHANNEL.messageBuilder(ClusterStatusRequestPacket.class, index++, NetworkDirection.PLAY_TO_SERVER)
                .encoder(ClusterStatusRequestPacket::encode)
                .decoder(ClusterStatusRequestPacket::decode)
                .consumerMainThread((message, context) -> message.handle(context))
                .add();
        CHANNEL.messageBuilder(ClusterStatusResponsePacket.class, index, NetworkDirection.PLAY_TO_CLIENT)
                .encoder(ClusterStatusResponsePacket::encode)
                .decoder(ClusterStatusResponsePacket::decode)
                .consumerMainThread((message, context) -> message.handle(context))
                .add();
        LOGGER.debug("Registered Forge Talker network channel {} (protocol {})",
                CHANNEL_NAME, PROTOCOL_VERSION);
    }

    /**
     * Sends the current node status to one player.
     *
     * <h2>Threading</h2>
     * Called from the server main thread (packet handler or command). Reading the runtime is safe
     * from any thread, but sending is kept on the main thread to match Forge's expectations.
     *
     * @param player recipient, never {@code null}
     */
    public static void sendStatusTo(ServerPlayer player) {
        ForgeTalkerRuntime runtime = ForgeTalkerRuntime.get();
        if (runtime == null) {
            return;
        }
        ClusterStatusResponsePacket response = new ClusterStatusResponsePacket(
                runtime.nodeId().value(),
                runtime.dispatcher().lastTransportState(),
                runtime.registry().chunksOwnedBy(runtime.nodeId()).size(),
                runtime.dispatcher().deferredCount(),
                runtime.registry().currentEpoch(),
                runtime.metrics().summary());
        CHANNEL.send(PacketDistributor.PLAYER.with(() -> player), response);
    }

    /**
     * @return the underlying Forge channel, exposed for integration tests
     */
    public static SimpleChannel channel() {
        return CHANNEL;
    }
}
