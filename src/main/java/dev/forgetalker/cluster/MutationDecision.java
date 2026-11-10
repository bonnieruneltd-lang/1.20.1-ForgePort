package dev.forgetalker.cluster;

/**
 * Tri-state outcome of an ownership check.
 *
 * <h2>Operational purpose</h2>
 * Two states (allowed / denied) are not enough in a cluster: a denied mutation is either
 * <em>relayable</em> (another node owns the chunk and can apply it) or <em>unresolvable</em>
 * (nobody owns it yet, the chunk is not loaded anywhere, or the bridge is down). Collapsing those
 * cases would either drop legitimate block changes or spam the bus with undeliverable frames.
 *
 * <h2>Thread safety</h2>
 * Enums are immutable; safe from any thread.
 */
public enum MutationDecision {

    /**
     * The local node owns the chunk/entity, or the cluster is inactive: execute locally.
     */
    ALLOW,

    /**
     * A different, reachable node owns the chunk/entity: cancel locally and forward the mutation.
     */
    RELAY,

    /**
     * Ownership cannot be resolved (or the chunk is unloaded): cancel and drop the mutation.
     * Vanilla would discard such writes as well, so this matches single-node semantics.
     */
    REJECT
}
