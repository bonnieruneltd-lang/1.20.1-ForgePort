package dev.forgetalker.cluster;

import java.util.Objects;

/**
 * Verdict returned by {@link OwnershipPolicy} for a mutation that one node is about to perform.
 *
 * <h2>Operational purpose</h2>
 * Minecraft exposes many mutation entry points ({@code Level.setBlock}, {@code ServerLevel.addFreshEntity},
 * block-break events, modded payloads). All of them must reach the same conclusion using the same
 * rules, otherwise a cluster develops "split brain" writes where two nodes mutate the same chunk.
 * A verdict bundles the decision with a human readable reason so that debug logs and
 * {@code /forgetalker status} can explain <em>why</em> a mutation was relayed or rejected.
 *
 * <h2>Thread safety</h2>
 * Immutable record; safe to share between the server thread (where the guards run) and background
 * threads that aggregate metrics.
 *
 * @param decision the tri-state decision, never {@code null}
 * @param reason   short machine-friendly explanation used in logs, never {@code null}
 */
public record MutationVerdict(MutationDecision decision, String reason) {

    /**
     * Canonical constructor with null checks.
     *
     * @throws NullPointerException if either component is {@code null}
     */
    public MutationVerdict {
        Objects.requireNonNull(decision, "decision");
        Objects.requireNonNull(reason, "reason");
    }

    /**
     * Creates an {@link MutationDecision#ALLOW} verdict.
     *
     * @param reason why the mutation may proceed locally, never {@code null}
     * @return the verdict, never {@code null}
     */
    public static MutationVerdict allow(String reason) {
        return new MutationVerdict(MutationDecision.ALLOW, reason);
    }

    /**
     * Creates a {@link MutationDecision#RELAY} verdict: the local mutation must be cancelled and
     * forwarded to the node that owns the chunk.
     *
     * @param reason why the mutation must be relayed, never {@code null}
     * @return the verdict, never {@code null}
     */
    public static MutationVerdict relay(String reason) {
        return new MutationVerdict(MutationDecision.RELAY, reason);
    }

    /**
     * Creates a {@link MutationDecision#REJECT} verdict: the mutation is cancelled and dropped
     * because no node can legally own it (unknown owner, unloaded chunk, dead session).
     *
     * @param reason why the mutation must be dropped, never {@code null}
     * @return the verdict, never {@code null}
     */
    public static MutationVerdict reject(String reason) {
        return new MutationVerdict(MutationDecision.REJECT, reason);
    }

    /**
     * @return {@code true} when the caller may execute the mutation locally
     */
    public boolean isAllowed() {
        return decision == MutationDecision.ALLOW;
    }

    /**
     * @return {@code true} when the caller must forward the mutation instead of executing it
     */
    public boolean isRelayed() {
        return decision == MutationDecision.RELAY;
    }

    /**
     * @return {@code true} when the caller must drop the mutation entirely
     */
    public boolean isRejected() {
        return decision == MutationDecision.REJECT;
    }

    /**
     * @return {@code decision + ": " + reason}, used verbatim in debug logs
     */
    @Override
    public String toString() {
        return decision + ": " + reason;
    }
}
