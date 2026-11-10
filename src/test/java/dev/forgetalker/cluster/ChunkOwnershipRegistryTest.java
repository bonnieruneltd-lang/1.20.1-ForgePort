package dev.forgetalker.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the epoch rules of the ownership mirror.
 *
 * <p>The mirror is what every guard consults, so its staleness rules are safety critical: a stale
 * snapshot that wins over a newer delta would make two nodes believe they own the same chunk.
 */
class ChunkOwnershipRegistryTest {

    private static final ClusterNodeId A = new ClusterNodeId("node-a");
    private static final ClusterNodeId B = new ClusterNodeId("node-b");
    private static final ChunkCoordinate CHUNK = new ChunkCoordinate("minecraft:overworld", 3, -7);

    /**
     * A snapshot replaces the whole table and is queryable immediately.
     */
    @Test
    @DisplayName("snapshot replaces the table")
    void snapshotApplies() {
        ChunkOwnershipRegistry registry = new ChunkOwnershipRegistry();
        registry.applyUpdate(1L, CHUNK, A);
        assertTrue(registry.applySnapshot(2L, Map.of(CHUNK, B)));
        assertEquals(B, registry.ownerOf(CHUNK));
        assertEquals(2L, registry.currentEpoch());
        assertEquals(1, registry.size());
    }

    /**
     * Older snapshots must never overwrite newer state.
     */
    @Test
    @DisplayName("stale snapshots are ignored")
    void staleSnapshotIgnored() {
        ChunkOwnershipRegistry registry = new ChunkOwnershipRegistry();
        registry.applySnapshot(5L, Map.of(CHUNK, A));
        assertFalse(registry.applySnapshot(4L, Map.of(CHUNK, B)));
        assertEquals(A, registry.ownerOf(CHUNK));
        assertEquals(5L, registry.currentEpoch());
    }

    /**
     * Deltas assign, reassign and release chunks.
     */
    @Test
    @DisplayName("deltas assign and release")
    void deltasApply() {
        ChunkOwnershipRegistry registry = new ChunkOwnershipRegistry();
        assertTrue(registry.applyUpdate(1L, CHUNK, A));
        assertTrue(registry.isOwner(CHUNK, A));
        assertTrue(registry.applyUpdate(2L, CHUNK, B));
        assertTrue(registry.isOwner(CHUNK, B));
        assertTrue(registry.applyUpdate(3L, CHUNK, null));
        assertNull(registry.ownerOf(CHUNK));
        assertEquals(0, registry.size());
    }

    /**
     * An out-of-order delta is dropped.
     */
    @Test
    @DisplayName("stale deltas are ignored")
    void staleDeltaIgnored() {
        ChunkOwnershipRegistry registry = new ChunkOwnershipRegistry();
        registry.applyUpdate(10L, CHUNK, A);
        assertFalse(registry.applyUpdate(9L, CHUNK, B));
        assertEquals(A, registry.ownerOf(CHUNK));
    }

    /**
     * Filtering by owner is used by the status command and must not include foreign chunks.
     */
    @Test
    @DisplayName("chunksOwnedBy filters by node")
    void filterByOwner() {
        ChunkOwnershipRegistry registry = new ChunkOwnershipRegistry();
        ChunkCoordinate other = new ChunkCoordinate("minecraft:overworld", 4, -7);
        registry.applyUpdate(1L, CHUNK, A);
        registry.applyUpdate(2L, other, B);
        assertEquals(Map.of(CHUNK, A), registry.chunksOwnedBy(A));
        assertEquals(Map.of(other, B), registry.chunksOwnedBy(B));
    }

    /**
     * Clearing resets the epoch so a restarted master can start numbering from zero again.
     */
    @Test
    @DisplayName("clear resets the epoch")
    void clearResetsEpoch() {
        ChunkOwnershipRegistry registry = new ChunkOwnershipRegistry();
        registry.applySnapshot(42L, Map.of(CHUNK, A));
        registry.clear();
        assertEquals(0L, registry.currentEpoch());
        assertTrue(registry.asMap().isEmpty());
        assertTrue(registry.applySnapshot(1L, Map.of(CHUNK, B)));
    }
}
