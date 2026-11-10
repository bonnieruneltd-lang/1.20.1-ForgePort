package dev.forgetalker.dispatch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the bounded deferral queue used for mutations whose chunk is not loaded yet.
 *
 * <p>The important properties are: FIFO order is preserved, a not-ready head blocks the queue (so two
 * changes to the same chunk cannot be reordered), the queue never exceeds its capacity, and the
 * eviction/drop counters stay accurate for {@code /forgetalker status}.
 */
class DeferredMutationQueueTest {

    /**
     * Ready entries are applied in insertion order and removed.
     */
    @Test
    @DisplayName("drain applies ready entries in FIFO order")
    void drainsInOrder() {
        DeferredMutationQueue<Integer> queue = new DeferredMutationQueue<>(10, value -> true);
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);
        List<Integer> applied = new ArrayList<>();
        int count = queue.drain(applied::add, 10);
        assertEquals(3, count);
        assertEquals(List.of(1, 2, 3), applied);
        assertEquals(0, queue.size());
        assertEquals(3L, queue.appliedCount());
    }

    /**
     * A not-ready head stops the drain so per-chunk ordering is preserved.
     */
    @Test
    @DisplayName("a blocked head stops the drain")
    void blockedHeadStopsDrain() {
        DeferredMutationQueue<Integer> queue = new DeferredMutationQueue<>(10, value -> value != 1);
        queue.offer(1);
        queue.offer(2);
        List<Integer> applied = new ArrayList<>();
        assertEquals(0, queue.drain(applied::add, 10));
        assertEquals(2, queue.size());
        assertTrue(applied.isEmpty());
    }

    /**
     * Eviction keeps the newest entries, which are the most valuable after a burst.
     */
    @Test
    @DisplayName("capacity evicts the oldest entries")
    void capacityEvictsOldest() {
        DeferredMutationQueue<Integer> queue = new DeferredMutationQueue<>(2, value -> true);
        queue.offer(1);
        queue.offer(2);
        assertTrue(queue.offer(3));
        assertEquals(2, queue.size());
        assertEquals(1L, queue.droppedCount());
        List<Integer> applied = new ArrayList<>();
        queue.drain(applied::add, 10);
        assertEquals(List.of(2, 3), applied);
    }

    /**
     * The per-call budget bounds the work done in a single server tick.
     */
    @Test
    @DisplayName("drain respects the budget")
    void budgetRespected() {
        DeferredMutationQueue<Integer> queue = new DeferredMutationQueue<>(10, value -> true);
        for (int i = 0; i < 5; i++) {
            queue.offer(i);
        }
        assertEquals(2, queue.drain(value -> { }, 2));
        assertEquals(3, queue.size());
    }
}
