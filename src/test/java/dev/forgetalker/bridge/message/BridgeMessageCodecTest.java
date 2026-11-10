package dev.forgetalker.bridge.message;

import dev.forgetalker.bridge.message.ControlMessages.Hello;
import dev.forgetalker.bridge.message.ControlMessages.HelloAck;
import dev.forgetalker.bridge.message.ControlMessages.PeerEndpoint;
import dev.forgetalker.bridge.message.ControlMessages.Ping;
import dev.forgetalker.bridge.message.ControlMessages.StatusRequest;
import dev.forgetalker.bridge.message.MutationMessages.BlockMutation;
import dev.forgetalker.bridge.message.MutationMessages.ClusterEvent;
import dev.forgetalker.bridge.message.MutationMessages.EntitySpawn;
import dev.forgetalker.bridge.message.MutationMessages.PayloadRelay;
import dev.forgetalker.bridge.message.MutationMessages.PlayerPresence;
import dev.forgetalker.bridge.message.MutationMessages.PresenceKind;
import dev.forgetalker.bridge.message.MutationMessages.RoutedFrame;
import dev.forgetalker.bridge.message.OwnershipMessages.ChunkInterest;
import dev.forgetalker.bridge.message.OwnershipMessages.ChunkOwnershipEntry;
import dev.forgetalker.bridge.message.OwnershipMessages.InterestKind;
import dev.forgetalker.bridge.message.OwnershipMessages.OwnershipSnapshot;
import dev.forgetalker.bridge.message.OwnershipMessages.OwnershipUpdate;
import dev.forgetalker.cluster.ChunkCoordinate;
import dev.forgetalker.cluster.ClusterNodeId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip and rejection tests for the cluster wire codec.
 *
 * <p>Every frame type is encoded, decoded and compared field by field. This is the only automated
 * protection against the most expensive kind of cluster bug: a frame that serialises fine but loses a
 * field, which only shows up as "sometimes a block change is applied at y=0" in production.
 */
class BridgeMessageCodecTest {

    private static final ClusterNodeId NODE = new ClusterNodeId("node-a");
    private static final ChunkCoordinate CHUNK = ChunkCoordinate.of("minecraft:overworld", -2, 9);

    /**
     * Encodes and decodes one frame, asserting equality of the record (records compare by value).
     *
     * @param message frame to round-trip
     * @param <T>     frame type
     * @return the decoded frame
     */
    private static <T extends BridgeMessage> T roundTrip(T message) {
        BridgeMessage decoded = BridgeMessageCodec.decode(BridgeMessageCodec.encode(message));
        assertEquals(message, decoded);
        return message;
    }

    /**
     * The handshake carries the secret and the peer port.
     */
    @Test
    @DisplayName("HELLO round-trips")
    void helloRoundTrip() {
        roundTrip(new Hello(NODE.value(), "s3cret", "1.0.0", 1, "server", 23546));
    }

    /**
     * The acknowledgement carries the epoch and peer endpoints.
     */
    @Test
    @DisplayName("HELLO_ACK round-trips with peers")
    void helloAckRoundTrip() {
        roundTrip(new HelloAck("master", "session-1", 1, 42L,
                List.of(new PeerEndpoint("node-b", "10.0.0.2", 23546))));
    }

    /**
     * Ownership snapshots preserve chunk coordinates and owner ids, including negative coordinates.
     */
    @Test
    @DisplayName("OWNERSHIP_SNAPSHOT round-trips")
    void snapshotRoundTrip() {
        roundTrip(new OwnershipSnapshot(7L, List.of(new ChunkOwnershipEntry(CHUNK, NODE),
                new ChunkOwnershipEntry(ChunkCoordinate.of("minecraft:the_nether", 0, 0),
                        new ClusterNodeId("node-b")))));
    }

    /**
     * A release delta encodes a JSON null owner and decodes back to null.
     */
    @Test
    @DisplayName("OWNERSHIP_UPDATE round-trips assigned and released")
    void updateRoundTrip() {
        roundTrip(new OwnershipUpdate(8L, CHUNK, NODE));
        OwnershipUpdate released = roundTrip(new OwnershipUpdate(9L, CHUNK, null));
        assertNull(released.owner());
    }

    /**
     * Chunk interest hints survive the trip with their kind.
     */
    @Test
    @DisplayName("CHUNK_INTEREST round-trips")
    void interestRoundTrip() {
        roundTrip(new ChunkInterest(CHUNK, InterestKind.CLAIM));
        roundTrip(new ChunkInterest(CHUNK, InterestKind.RELEASE));
    }

    /**
     * Block mutations carry the global state id and flags needed to replay the write.
     */
    @Test
    @DisplayName("BLOCK_MUTATION round-trips")
    void blockMutationRoundTrip() {
        roundTrip(new BlockMutation(CHUNK, (CHUNK.chunkX() << 4) + 1, 70, (CHUNK.chunkZ() << 4) + 2,
                12345, 3, NODE.value()));
    }

    /**
     * Entity spawns carry optional NBT; both variants must survive.
     */
    @Test
    @DisplayName("ENTITY_SPAWN round-trips with and without NBT")
    void entitySpawnRoundTrip() {
        UUID uuid = UUID.randomUUID();
        roundTrip(new EntitySpawn(CHUNK, uuid, "minecraft:zombie", 1.5D, 64.0D, -3.5D, 90.0F, 12.0F,
                null, NODE.value()));
        EntitySpawn withNbt = roundTrip(new EntitySpawn(CHUNK, uuid, "minecraft:zombie", 1.5D, 64.0D,
                -3.5D, 90.0F, 12.0F, new byte[]{1, 2, 3, 4}, NODE.value()));
        assertArrayEquals(new byte[]{1, 2, 3, 4}, withNbt.nbt());
    }

    /**
     * Relayed payloads are arbitrary binary data and must not be re-encoded as text.
     */
    @Test
    @DisplayName("PAYLOAD_RELAY round-trips binary data")
    void payloadRelayRoundTrip() {
        byte[] payload = new byte[] {(byte) 0xFF, (byte) 0xFE, 0, 1, 2, 3};
        PayloadRelay relay = roundTrip(new PayloadRelay(UUID.randomUUID(), "somemod:packet", payload,
                0, NODE.value()));
        assertArrayEquals(payload, relay.payload());
    }

    /**
     * Presence and cluster events are informational but must not lose identifiers.
     */
    @Test
    @DisplayName("PLAYER_PRESENCE and CLUSTER_EVENT round-trip")
    void informationalRoundTrip() {
        roundTrip(new PlayerPresence(UUID.randomUUID(), "Steve", PresenceKind.JOIN, NODE.value()));
        roundTrip(new ClusterEvent("block_break", "minecraft:overworld", 10, 64, -10,
                UUID.randomUUID(), "minecraft:stone", 1234L));
    }

    /**
     * Routed frames nest another frame; the nesting must be preserved exactly.
     */
    @Test
    @DisplayName("ROUTED_FRAME round-trips its inner frame")
    void routedFrameRoundTrip() {
        roundTrip(new RoutedFrame("node-b", new BlockMutation(CHUNK, CHUNK.minBlockX(), 0,
                CHUNK.minBlockZ(), 1, 3, NODE.value())));
    }

    /**
     * Status frames and heartbeats must round-trip for the command and watchdog to work.
     */
    @Test
    @DisplayName("PING and STATUS_REQUEST round-trip")
    void controlRoundTrip() {
        roundTrip(new Ping(99L, 1700000000000L));
        roundTrip(new StatusRequest(5L, NODE.value()));
    }

    /**
     * An unknown discriminator must be reported as "newer protocol", not as corruption, so a rolling
     * upgrade only produces debug logs.
     */
    @Test
    @DisplayName("unknown frame types are reported as unknown, not corrupt")
    void unknownType() {
        BridgeProtocolException error = assertThrows(BridgeProtocolException.class, () ->
                BridgeMessageCodec.decode("{\"type\":\"from_the_future\"}".getBytes(StandardCharsets.UTF_8)));
        assertTrue(error.isUnknownFrameType());
    }

    /**
     * Frames naming a block outside their declared chunk are rejected at construction time.
     */
    @Test
    @DisplayName("inconsistent block mutations are rejected")
    void inconsistentBlockMutation() {
        assertThrows(IllegalArgumentException.class, () -> new BlockMutation(CHUNK, 0, 64, 0, 1, 3,
                NODE.value()));
    }

    /**
     * A frame larger than the protocol limit must be rejected instead of exhausting memory.
     */
    @Test
    @DisplayName("oversized frames are rejected")
    void oversizedFrame() {
        byte[] huge = new byte[BridgeMessageCodec.MAX_FRAME_BYTES + 1];
        BridgeProtocolException error = assertThrows(BridgeProtocolException.class,
                () -> BridgeMessageCodec.decode(huge));
        assertTrue(error.getMessage().contains("too large"));
    }

    /**
     * Empty input is corrupt input.
     */
    @Test
    @DisplayName("empty frames are rejected")
    void emptyFrame() {
        assertThrows(BridgeProtocolException.class, () -> BridgeMessageCodec.decode(new byte[0]));
    }
}
