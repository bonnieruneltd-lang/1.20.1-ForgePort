package dev.forgetalker.bridge;

import java.util.concurrent.atomic.LongAdder;

/**
 * Lock-free counters describing what the bridge is doing.
 *
 * <h2>Operational purpose</h2>
 * Multi-node bugs are rarely reproducible on demand. These counters let an operator (or an AI agent
 * debugging a ticket) answer "did the bridge actually relay anything?" from {@code /forgetalker
 * status} without enabling debug logging, which is too noisy for production.
 *
 * <h2>Thread safety</h2>
 * Uses {@link LongAdder}: increments from the server thread, the Netty event loop and the command
 * thread are safe and contention-free.
 */
public final class ClusterMetrics {

    private final LongAdder framesInbound = new LongAdder();
    private final LongAdder framesOutbound = new LongAdder();
    private final LongAdder decodeErrors = new LongAdder();
    private final LongAdder blockedMutations = new LongAdder();
    private final LongAdder relayedBlockMutations = new LongAdder();
    private final LongAdder relayedEntitySpawns = new LongAdder();
    private final LongAdder relayedPayloads = new LongAdder();
    private final LongAdder deferredApplied = new LongAdder();
    private final LongAdder ownershipSnapshots = new LongAdder();
    private final LongAdder ownershipUpdates = new LongAdder();
    private final LongAdder reconnectAttempts = new LongAdder();

    /**
     * Records one decoded inbound frame.
     */
    public void recordInboundFrame() {
        framesInbound.increment();
    }

    /**
     * Records one encoded outbound frame.
     */
    public void recordOutboundFrame() {
        framesOutbound.increment();
    }

    /**
     * Records one frame that could not be decoded.
     */
    public void recordDecodeError() {
        decodeErrors.increment();
    }

    /**
     * Records one mutation that the guard refused to execute locally.
     */
    public void recordBlockedMutation() {
        blockedMutations.increment();
    }

    /**
     * Records one block mutation that was forwarded to the owning node.
     */
    public void recordRelayedBlockMutation() {
        relayedBlockMutations.increment();
    }

    /**
     * Records one entity spawn that was forwarded to the owning node.
     */
    public void recordRelayedEntitySpawn() {
        relayedEntitySpawns.increment();
    }

    /**
     * Records one modded payload that was forwarded to the peers.
     */
    public void recordRelayedPayload() {
        relayedPayloads.increment();
    }

    /**
     * Records one deferred mutation that was finally applied.
     */
    public void recordDeferredApplied() {
        deferredApplied.increment();
    }

    /**
     * Records one applied ownership snapshot.
     */
    public void recordOwnershipSnapshot() {
        ownershipSnapshots.increment();
    }

    /**
     * Records one applied ownership delta.
     */
    public void recordOwnershipUpdate() {
        ownershipUpdates.increment();
    }

    /**
     * Records one reconnect attempt of the master link.
     */
    public void recordReconnectAttempt() {
        reconnectAttempts.increment();
    }

    /**
     * @return a multi-line, human readable summary used by {@code /forgetalker status}
     */
    public String summary() {
        return "frames in=" + framesInbound.sum() + " out=" + framesOutbound.sum()
                + ", decodeErrors=" + decodeErrors.sum()
                + ", blocked=" + blockedMutations.sum()
                + ", relayedBlocks=" + relayedBlockMutations.sum()
                + ", relayedEntities=" + relayedEntitySpawns.sum()
                + ", relayedPayloads=" + relayedPayloads.sum()
                + ", deferredApplied=" + deferredApplied.sum()
                + ", snapshots=" + ownershipSnapshots.sum()
                + ", ownershipUpdates=" + ownershipUpdates.sum()
                + ", reconnects=" + reconnectAttempts.sum();
    }

    /**
     * @return inbound frame count
     */
    public long framesInbound() {
        return framesInbound.sum();
    }

    /**
     * @return outbound frame count
     */
    public long framesOutbound() {
        return framesOutbound.sum();
    }

    /**
     * @return decode error count
     */
    public long decodeErrors() {
        return decodeErrors.sum();
    }

    /**
     * @return blocked (relayed or rejected) mutation count
     */
    public long blockedMutations() {
        return blockedMutations.sum();
    }
}
