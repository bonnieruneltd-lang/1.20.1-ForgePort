package dev.forgetalker.mixin;

import dev.forgetalker.network.ModdedPayloadRelay;
import dev.forgetalker.util.MixinProbe;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.network.protocol.game.ServerboundCustomPayloadPacket;
import net.minecraft.server.network.ServerGamePacketListenerImpl;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * Observes modded custom payloads so they can be relayed onto the cluster bus.
 *
 * <h2>Why this injection point</h2>
 * {@code ServerGamePacketListenerImpl.handleCustomPayload} is the only server-side handler for
 * {@code ServerboundCustomPayloadPacket} in 1.20.1, and it is patched by Forge to dispatch the
 * payload into every registered {@code SimpleChannel}. Injecting at {@code HEAD} lets Forge Talker see
 * the raw channel name and bytes before any mod consumes them. A Netty pipeline handler would have to
 * sit in front of Minecraft's own decoder and would break with every protocol change.
 *
 * <h2>Why non-cancelling</h2>
 * The payload must still be handled locally: the node the player is connected to may legitimately
 * own the player's chunk. This is an <em>observer</em>, not an interceptor. Relaying is decided
 * inside {@link ModdedPayloadRelay}, which also filters vanilla/Forge channels and enforces the size
 * cap.
 *
 * <h2>Thread safety</h2>
 * Runs on the Netty event loop. The handler copies the bytes (non-destructively, via
 * {@code getBytes(readerIndex, ...)}) and calls into the relay, which only enqueues a frame. It never
 * touches world state.
 *
 * <h2>Cluster context</h2>
 * Every eligible payload becomes a {@code PAYLOAD_RELAY} frame and is delivered to local players on
 * the peer nodes via {@code NetworkHooks.onCustomPayload}. Hop counting prevents broadcast loops.
 */
@Mixin(ServerGamePacketListenerImpl.class)
public abstract class ServerGamePacketListenerImplMixin {

    static {
        MixinProbe.markApplied("ServerGamePacketListenerImpl.handleCustomPayload");
    }

    /**
     * Copies the payload bytes and offers them to the relay.
     *
     * @param packet payload packet, never {@code null}
     * @param ci     callback info, unused (the vanilla handler continues)
     */
    @Inject(method = "handleCustomPayload", at = @At("HEAD"))
    private void forgetalker$observeCustomPayload(ServerboundCustomPayloadPacket packet, CallbackInfo ci) {
        ServerGamePacketListenerImpl listener = (ServerGamePacketListenerImpl) (Object) this;
        FriendlyByteBuf data = packet.getData();
        if (data == null) {
            return;
        }
        byte[] bytes = new byte[data.readableBytes()];
        // getBytes() with an absolute index leaves the reader index untouched, so Forge's own
        // dispatch of the same packet still sees the full payload.
        data.getBytes(data.readerIndex(), bytes);
        ModdedPayloadRelay.observeInboundCustomPayload(listener.player, packet.getIdentifier(), bytes);
    }
}
