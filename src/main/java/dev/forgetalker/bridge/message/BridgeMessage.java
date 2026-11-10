package dev.forgetalker.bridge.message;

/**
 * Marker type for every frame that can travel on the Forge Talker cluster bus.
 *
 * <h2>Operational purpose</h2>
 * The bus carries control-plane frames (handshake, heartbeat, ownership) and data-plane frames
 * (block mutations, entity spawns, relayed mod payloads). Modelling them as one sealed hierarchy
 * lets {@code PeerMessageDispatcher} use an exhaustive {@code switch} - a new frame type cannot be
 * added without the compiler pointing at every place that must handle it. That is the main defence
 * against the "silently dropped message" class of cluster bugs.
 *
 * <h2>Thread safety</h2>
 * All implementations are immutable records. Frames are created on the server thread, encoded on
 * the Netty event loop and decoded there too, then handed back to the server thread through the
 * dispatcher. Because records are immutable, no defensive copying is required.
 *
 * <h2>Cluster context</h2>
 * Every frame is node-agnostic: addressing is done by the transport (master relay or direct peer
 * link). See {@code docs/PROTOCOL.md} for the JSON layout of each subtype.
 */
public sealed interface BridgeMessage
        permits ControlMessages.Hello,
                ControlMessages.HelloAck,
                ControlMessages.Reject,
                ControlMessages.Ping,
                ControlMessages.Pong,
                ControlMessages.PeerAdvertisement,
                ControlMessages.StatusRequest,
                ControlMessages.StatusResponse,
                ControlMessages.Goodbye,
                OwnershipMessages.OwnershipSnapshot,
                OwnershipMessages.OwnershipUpdate,
                OwnershipMessages.ChunkInterest,
                MutationMessages.BlockMutation,
                MutationMessages.EntitySpawn,
                MutationMessages.PayloadRelay,
                MutationMessages.PlayerPresence,
                MutationMessages.ClusterEvent,
                MutationMessages.RoutedFrame {

    /**
     * @return the frame kind, used as the JSON discriminator and for logging. Never {@code null}.
     */
    MessageType type();
}
