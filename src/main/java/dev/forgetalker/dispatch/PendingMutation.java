package dev.forgetalker.dispatch;

import dev.forgetalker.bridge.message.BridgeMessage;
import dev.forgetalker.bridge.message.MutationMessages.BlockMutation;
import dev.forgetalker.bridge.message.MutationMessages.EntitySpawn;
import dev.forgetalker.cluster.ChunkCoordinate;

import java.util.Objects;

/**
 * A relayed mutation that is waiting for its chunk to become loaded.
 *
 * <h2>Operational purpose</h2>
 * Block changes and entity spawns arrive from peers before this node may have the target chunk in
 * memory (the master hands ownership over asynchronously). Wrapping the frame together with its
 * chunk lets the deferral queue answer "can this be applied yet?" without re-parsing the message.
 *
 * <h2>Thread safety</h2>
 * Immutable record; created and consumed on the server thread only.
 *
 * @param chunk   chunk the mutation targets, never {@code null}
 * @param message the original frame ({@link BlockMutation} or {@link EntitySpawn}), never {@code null}
 */
public record PendingMutation(ChunkCoordinate chunk, BridgeMessage message) {

    /**
     * Validates that the frame is one of the deferrable types.
     *
     * @throws IllegalArgumentException if the frame type cannot be deferred
     */
    public PendingMutation {
        Objects.requireNonNull(chunk, "chunk");
        Objects.requireNonNull(message, "message");
        if (!(message instanceof BlockMutation) && !(message instanceof EntitySpawn)) {
            throw new IllegalArgumentException("Not a deferrable mutation: " + message.type());
        }
    }
}
