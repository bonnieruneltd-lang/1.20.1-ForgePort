package dev.forgetalker.dispatch;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Bounded FIFO queue for mutations that could not be applied yet (chunk not loaded, ownership delta
 * still in flight).
 *
 * <h2>Operational purpose</h2>
 * A relayed block change may arrive before the target node has loaded the chunk. Dropping it would
 * lose the mutation permanently (the origin node already cancelled it); buffering it and retrying
 * each tick keeps the cluster consistent. The queue is bounded so a pathological peer cannot exhaust
 * the heap: when full, the <em>oldest</em> entry is dropped and counted, because stale mutations are
 * less valuable than fresh ones.
 *
 * <h2>Thread safety</h2>
 * Not internally synchronised: the queue is only touched on the server thread (dispatcher apply and
 * tick drain). The {@link AtomicLong} counters are safe to read from any thread for reporting.
 *
 * @param <T> mutation type held by the queue
 */
public final class DeferredMutationQueue<T> {

    private final int capacity;
    private final Deque<T> entries;
    private final AtomicLong droppedCount = new AtomicLong();
    private final AtomicLong appliedCount = new AtomicLong();
    private final Predicate<T> readyCheck;

    /**
     * @param capacity   maximum number of buffered mutations, must be positive
     * @param readyCheck predicate telling whether an entry can be applied now; evaluated on the
     *                   server thread during {@link #drain()}
     */
    public DeferredMutationQueue(int capacity, Predicate<T> readyCheck) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1");
        }
        this.capacity = capacity;
        this.readyCheck = Objects.requireNonNull(readyCheck, "readyCheck");
        this.entries = new ArrayDeque<>(Math.min(capacity, 256));
    }

    /**
     * Adds a mutation, evicting the oldest entry when the queue is full.
     *
     * @param mutation mutation to buffer, never {@code null}
     * @return {@code true} when the queue evicted an older entry to make room
     */
    public boolean offer(T mutation) {
        Objects.requireNonNull(mutation, "mutation");
        boolean evicted = false;
        while (entries.size() >= capacity) {
            entries.pollFirst();
            droppedCount.incrementAndGet();
            evicted = true;
        }
        entries.addLast(mutation);
        return evicted;
    }

    /**
     * Applies every entry whose predicate currently holds, preserving FIFO order and stopping at the
     * first entry that is not ready (so ordering between mutations of the same chunk is preserved).
     *
     * @param applier consumer invoked once per ready entry, never {@code null}
     * @param budget  maximum number of entries to apply in this call, must be positive
     * @return the number of applied entries
     */
    public int drain(java.util.function.Consumer<T> applier, int budget) {
        Objects.requireNonNull(applier, "applier");
        if (budget < 1) {
            throw new IllegalArgumentException("budget must be >= 1");
        }
        int applied = 0;
        while (applied < budget) {
            T head = entries.peekFirst();
            if (head == null || !readyCheck.test(head)) {
                break;
            }
            entries.pollFirst();
            applier.accept(head);
            applied++;
            appliedCount.incrementAndGet();
        }
        return applied;
    }

    /**
     * @return the number of buffered mutations
     */
    public int size() {
        return entries.size();
    }

    /**
     * @return the maximum number of buffered mutations
     */
    public int capacity() {
        return capacity;
    }

    /**
     * @return total entries evicted because the queue was full
     */
    public long droppedCount() {
        return droppedCount.get();
    }

    /**
     * @return total entries successfully applied
     */
    public long appliedCount() {
        return appliedCount.get();
    }

    /**
     * Removes all entries without applying them. Called when the cluster session ends, because the
     * ownership data that would have validated them is gone.
     */
    public void clear() {
        entries.clear();
    }
}
