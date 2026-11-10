package dev.forgetalker.bridge.net;

import dev.forgetalker.bridge.message.BridgeMessage;
import dev.forgetalker.bridge.message.BridgeMessageCodec;
import dev.forgetalker.bridge.message.BridgeProtocolException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;
import java.util.function.Consumer;

/**
 * Netty frame codecs for the cluster bus.
 *
 * <h2>Wire framing</h2>
 * Each frame is a 4-byte big-endian length followed by that many UTF-8 JSON bytes. The length prefix
 * lets Netty reassemble TCP segments without the JSON parser seeing partial input, which is the
 * single most common source of "works on localhost, breaks over the internet" bugs in custom
 * protocols. {@code LengthFieldBasedFrameDecoder(MAX, 0, 4, 0, 4)} in the transport strips it again.
 *
 * <h2>Thread safety</h2>
 * Netty instantiates one handler per channel; handlers are confined to their channel's event loop.
 * The encoder/decoder themselves are stateless apart from the injected callbacks, and
 * {@link BridgeMessageCodec} is stateless, so no synchronisation is needed.
 *
 * <h2>Error policy</h2>
 * A malformed frame is reported to the transport and dropped; it must not close the channel, because
 * one bad frame (or one frame from a newer protocol version) must not tear down the whole cluster
 * session.
 */
public final class BridgeFrameCodec {

    /** Name of the length-field handler in the pipeline. */
    public static final String LENGTH_HANDLER = "forgetalker-length";

    /** Name of the decoder handler in the pipeline. */
    public static final String DECODER_HANDLER = "forgetalker-decoder";

    /** Name of the encoder handler in the pipeline. */
    public static final String ENCODER_HANDLER = "forgetalker-encoder";

    private BridgeFrameCodec() {
        // Holder class.
    }

    /**
     * Decodes one length-delimited JSON frame into a {@link BridgeMessage}.
     */
    public static final class Decoder extends MessageToMessageDecoder<ByteBuf> {

        private final Consumer<Throwable> errorSink;

        /**
         * @param errorSink receives decode failures, never {@code null}
         */
        public Decoder(Consumer<Throwable> errorSink) {
            this.errorSink = errorSink;
        }

        /**
         * Converts the frame payload to a message without retaining the buffer.
         *
         * @param ctx     channel context, unused
         * @param msg     frame payload with the length prefix already stripped
         * @param out     receives the decoded frame so the session handler sees it
         * @throws BridgeProtocolException never thrown; failures go to {@code errorSink}
         */
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
            byte[] bytes = new byte[msg.readableBytes()];
            msg.getBytes(msg.readerIndex(), bytes);
            try {
                out.add(BridgeMessageCodec.decode(bytes));
            } catch (BridgeProtocolException e) {
                errorSink.accept(e);
            } catch (RuntimeException e) {
                errorSink.accept(BridgeProtocolException.corrupt("Unexpected decode failure", e));
            }
        }
    }

    /**
     * Encodes a {@link BridgeMessage} as {@code [int length][json bytes]}.
     */
    public static final class Encoder extends MessageToMessageEncoder<BridgeMessage> {

        /**
         * @param ctx channel context providing the allocator
         * @param msg frame to encode
         * @param out receives exactly one buffer
         */
        @Override
        protected void encode(ChannelHandlerContext ctx, BridgeMessage msg, List<Object> out) {
            byte[] payload = BridgeMessageCodec.encode(msg);
            ByteBuf buffer = ctx.alloc().buffer(4 + payload.length);
            buffer.writeInt(payload.length);
            buffer.writeBytes(payload);
            out.add(buffer);
        }
    }
}
