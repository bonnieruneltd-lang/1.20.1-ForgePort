package dev.forgetalker.network.packet;

import dev.forgetalker.network.ForgeTalkerNetwork;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.server.level.ServerPlayer;
import net.minecraftforge.network.NetworkEvent;

import java.util.function.Supplier;

/**
 * Client -&gt; server request for the cluster status of the node the player is connected to.
 *
 * <h2>Operational purpose</h2>
 * Lets a client-side Forge Talker build show which node the player is on and whether that node owns
 * the player's chunk. It also gives integration tests a way to exercise the {@code SimpleChannel}
 * without requiring an operator command.
 *
 * <h2>Thread safety</h2>
 * The payload is an empty marker, so encode/decode are pure. {@link #handle} runs on the main server
 * thread via {@code consumerMainThread}; the reply is sent from there.
 */
public record ClusterStatusRequestPacket() {

    /** Shared stateless instance; the frame carries no data. */
    public static final ClusterStatusRequestPacket INSTANCE = new ClusterStatusRequestPacket();

    /**
     * Writes nothing: the frame is a marker.
     *
     * @param buffer target buffer, never {@code null}
     */
    public void encode(FriendlyByteBuf buffer) {
        // No payload.
    }

    /**
     * Reads nothing and returns the shared instance.
     *
     * @param buffer source buffer, never {@code null}
     * @return the shared instance, never {@code null}
     */
    public static ClusterStatusRequestPacket decode(FriendlyByteBuf buffer) {
        return INSTANCE;
    }

    /**
     * Handles the request on the server main thread and answers with a
     * {@link ClusterStatusResponsePacket}.
     *
     * @param context Forge network context supplier, never {@code null}
     */
    public void handle(Supplier<NetworkEvent.Context> context) {
        NetworkEvent.Context ctx = context.get();
        ctx.enqueueWork(() -> {
            ServerPlayer sender = ctx.getSender();
            if (sender == null) {
                return;
            }
            ForgeTalkerNetwork.sendStatusTo(sender);
        });
        ctx.setPacketHandled(true);
    }
}
