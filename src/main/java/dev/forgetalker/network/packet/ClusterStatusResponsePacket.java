package dev.forgetalker.network.packet;

import net.minecraft.network.FriendlyByteBuf;
import net.minecraftforge.network.NetworkEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Supplier;

/**
 * Server -&gt; client snapshot of the node's cluster status.
 *
 * <h2>Operational purpose</h2>
 * Lets a client display which node owns its chunk and whether that node is cluster-authoritative.
 * Purely informational: no client-side world state is modified, so the packet is also safe to send
 * to clients that do not have Forge Talker installed (they simply never request it).
 *
 * <h2>Thread safety</h2>
 * Immutable record. Decoding happens on the Netty thread, {@link #handle} runs on the client main
 * thread (Forge's {@code consumerMainThread}) and only logs.
 *
 * @param nodeId      node the client is connected to, never {@code null}
 * @param state       transport state description, never {@code null}
 * @param ownedChunks number of chunks the node owns according to its mirror
 * @param deferred    number of deferred mutations waiting for chunks to load
 * @param epoch       ownership epoch applied by the node
 * @param metrics     pre-formatted metrics summary, never {@code null}
 */
public record ClusterStatusResponsePacket(String nodeId,
                                          String state,
                                          int ownedChunks,
                                          int deferred,
                                          long epoch,
                                          String metrics) {

    private static final Logger LOGGER = LogManager.getLogger("ForgeTalker/Network");

    /**
     * Writes the snapshot to the buffer.
     *
     * @param buffer target buffer, never {@code null}
     */
    public void encode(FriendlyByteBuf buffer) {
        buffer.writeUtf(nodeId, 128);
        buffer.writeUtf(state, 512);
        buffer.writeVarInt(ownedChunks);
        buffer.writeVarInt(deferred);
        buffer.writeLong(epoch);
        buffer.writeUtf(metrics, 1024);
    }

    /**
     * Reads a snapshot from the buffer.
     *
     * @param buffer source buffer, never {@code null}
     * @return the decoded packet, never {@code null}
     */
    public static ClusterStatusResponsePacket decode(FriendlyByteBuf buffer) {
        return new ClusterStatusResponsePacket(buffer.readUtf(128), buffer.readUtf(512),
                buffer.readVarInt(), buffer.readVarInt(), buffer.readLong(), buffer.readUtf(1024));
    }

    /**
     * Logs the status on the client. Intentionally side-effect free so dedicated servers can send it
     * without a client-only class being loaded.
     *
     * @param context Forge network context supplier, never {@code null}
     */
    public void handle(Supplier<NetworkEvent.Context> context) {
        NetworkEvent.Context ctx = context.get();
        ctx.enqueueWork(() -> LOGGER.info(
                "Forge Talker node {} [{}] owns {} chunk(s), {} deferred, epoch {} | {}",
                nodeId, state, ownedChunks, deferred, epoch, metrics));
        ctx.setPacketHandled(true);
    }
}
