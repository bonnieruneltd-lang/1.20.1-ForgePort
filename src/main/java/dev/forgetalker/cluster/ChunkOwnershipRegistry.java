package dev.forgetalker.cluster;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe mirror of the MultiPaper master's chunk-ownership table.
 *
 * <h2>Operational purpose</h2>
 * The mixin guards need a sub-millisecond answer to "does this node own the chunk at this
 * position?". Querying the master per block update is impossible (network round trip per
 * {@code setBlock}), so the master's authoritative table is mirrored locally: a full snapshot on
 * handshake and incremental deltas afterwards.
 *
 * <h2>Consistency model</h2>
 * <ul>
 *   <li>The master assigns a monotonically increasing <em>epoch</em> to every ownership change.</li>
 *   <li>A snapshot or delta with an epoch lower than the local epoch is ignored. This makes
 *       reconnects safe: a stale snapshot replayed by a restarted master cannot resurrect old
 *       assignments.</li>
 *   <li>Snapshots are swapped in atomically (copy-on-write) so a reader never observes a partially
 *       filled table.</li>
 *   <li>Deltas are applied <em>in arrival order</em>. The transport guarantees ordering by
 *       dispatching frames sequentially on the server thread, which is why a delta does not need
 *       to rebuild the whole map.</li>
 *   <li>An absent entry means "the master has not assigned this chunk". Whether that is treated as
 *       local or foreign is decided by {@link OwnershipPolicy}, not here.</li>
 * </ul>
 *
 * <h2>Thread safety</h2>
 * Writes are serialized on an internal monitor and published through a {@code volatile} map
 * reference. Reads are lock-free {@link ConcurrentHashMap} lookups. Safe to call from the server
 * thread (guards, event handlers) and the Netty event loop (frame decoding) at the same time.
 */
public final class ChunkOwnershipRegistry {

    /**
     * Current ownership table. Snapshots replace this reference atomically; deltas mutate the
     * published concurrent map in place, which is safe for concurrent readers.
     */
    private volatile ConcurrentHashMap<ChunkCoordinate, ClusterNodeId> owners = new ConcurrentHashMap<>();

    /** Monotonic epoch of the last applied snapshot/delta. Guarded by {@code lock}. */
    private long epoch;

    /** Serialises snapshot swaps and epoch checks. Never held during map reads. */
    private final Object lock = new Object();

    /**
     * Replaces the whole ownership table with an authoritative snapshot.
     *
     * @param snapshotEpoch epoch assigned by the master to this snapshot
     * @param snapshot      chunk-to-node mapping; may be empty but not {@code null}
     * @return {@code true} if the snapshot was applied, {@code false} if it was stale
     * @implNote Runs on the server thread (dispatched from {@code PeerMessageDispatcher}) so that
     *         a guard can never observe a half-applied snapshot.
     */
    public boolean applySnapshot(long snapshotEpoch, Map<ChunkCoordinate, ClusterNodeId> snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        synchronized (lock) {
            if (snapshotEpoch < epoch) {
                return false;
            }
            epoch = snapshotEpoch;
            owners = new ConcurrentHashMap<>(snapshot);
            return true;
        }
    }

    /**
     * Applies one ownership delta (assignment or release).
     *
     * @param updateEpoch epoch assigned by the master to this delta
     * @param chunk       affected chunk, never {@code null}
     * @param newOwner    the node that now owns the chunk, or {@code null} to release it
     * @return {@code true} if the delta was applied, {@code false} if it was stale
     */
    public boolean applyUpdate(long updateEpoch, ChunkCoordinate chunk, ClusterNodeId newOwner) {
        Objects.requireNonNull(chunk, "chunk");
        final ConcurrentHashMap<ChunkCoordinate, ClusterNodeId> table;
        synchronized (lock) {
            if (updateEpoch < epoch) {
                return false;
            }
            epoch = updateEpoch;
            table = owners;
        }
        if (newOwner == null) {
            table.remove(chunk);
        } else {
            table.put(chunk, newOwner);
        }
        return true;
    }

    /**
     * Resolves the owning node of a chunk.
     *
     * @param chunk chunk to resolve, never {@code null}
     * @return the owning node, or {@code null} when the master has no assignment
     * @implNote Lock-free; called from the hot {@code Level.setBlock} mixin path.
     */
    public ClusterNodeId ownerOf(ChunkCoordinate chunk) {
        return owners.get(chunk);
    }

    /**
     * Convenience predicate used by the guards and the relay dispatcher.
     *
     * @param chunk chunk to test, never {@code null}
     * @param node  node to compare against, may be {@code null}
     * @return {@code true} only when {@code node} is non-null and currently owns {@code chunk}
     */
    public boolean isOwner(ChunkCoordinate chunk, ClusterNodeId node) {
        return node != null && node.equals(owners.get(chunk));
    }

    /**
     * Collects all chunks owned by one node. Used by {@code /forgetalker status} and by the
     * release-on-unload path to decide whether a claim is still needed.
     *
     * @param node node to filter by, never {@code null}
     * @return an immutable snapshot copy; never {@code null}
     * @implNote Iterates a concurrent map, so the result is a weakly consistent snapshot. That is
     *         acceptable for reporting; it must not be used to make ownership decisions.
     */
    public Map<ChunkCoordinate, ClusterNodeId> chunksOwnedBy(ClusterNodeId node) {
        Objects.requireNonNull(node, "node");
        return owners.entrySet().stream()
                .filter(entry -> node.equals(entry.getValue()))
                .collect(java.util.stream.Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * @return the number of chunks with a known owner; weakly consistent
     */
    public int size() {
        return owners.size();
    }

    /**
     * @return the highest epoch applied so far; used for diagnostics and tests
     */
    public long currentEpoch() {
        synchronized (lock) {
            return epoch;
        }
    }

    /**
     * Drops all ownership data. Called when the cluster session ends so that a subsequent
     * standalone period behaves like a normal single-node server.
     */
    public void clear() {
        synchronized (lock) {
            epoch = 0L;
            owners = new ConcurrentHashMap<>();
        }
    }

    /**
     * @return a defensive copy of all current assignments, for logging and tests
     */
    public Map<ChunkCoordinate, ClusterNodeId> asMap() {
        return Map.copyOf(owners);
    }
}