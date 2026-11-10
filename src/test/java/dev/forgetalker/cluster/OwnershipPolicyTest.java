package dev.forgetalker.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the ownership decision table.
 *
 * <p>These tests are the executable specification for "which node may mutate what". If a future
 * change alters the order of the checks, one of them fails and the corresponding javadoc in
 * {@link OwnershipPolicy} must be updated with it.
 */
class OwnershipPolicyTest {

    private static final ClusterNodeId SELF = new ClusterNodeId("node-a");
    private static final ClusterNodeId OTHER = new ClusterNodeId("node-b");

    /**
     * Client-side levels have no ownership data and must never be gated.
     */
    @Test
    @DisplayName("client side mutations are always allowed")
    void clientSideAllowed() {
        MutationVerdict verdict = OwnershipPolicy.evaluateBlockMutation(false, true, false, SELF, OTHER, false);
        assertTrue(verdict.isAllowed());
        assertEquals(OwnershipPolicy.REASON_CLIENT_SIDE, verdict.reason());
    }

    /**
     * Without an authoritative session the mod must behave like vanilla.
     */
    @Test
    @DisplayName("standalone mode allows foreign and unloaded chunks")
    void standaloneAllowed() {
        assertTrue(OwnershipPolicy.evaluateBlockMutation(true, false, false, SELF, OTHER, false).isAllowed());
        assertTrue(OwnershipPolicy.evaluateBlockMutation(true, false, false, null, null, false).isAllowed());
    }

    /**
     * A known foreign owner wins even when the local chunk is not loaded: relaying is not executing.
     */
    @Test
    @DisplayName("foreign owner is relayed even for unloaded chunks")
    void foreignOwnerRelayed() {
        MutationVerdict verdict = OwnershipPolicy.evaluateBlockMutation(true, true, false, SELF, OTHER, false);
        assertTrue(verdict.isRelayed());
        assertTrue(verdict.reason().contains(OTHER.value()));
    }

    /**
     * An unloaded chunk that this node owns cannot be written to; vanilla would discard it anyway.
     */
    @Test
    @DisplayName("locally owned but unloaded chunk is rejected")
    void localUnloadedRejected() {
        MutationVerdict verdict = OwnershipPolicy.evaluateBlockMutation(true, true, false, SELF, SELF, true);
        assertTrue(verdict.isRejected());
        assertEquals(OwnershipPolicy.REASON_CHUNK_UNLOADED, verdict.reason());
    }

    /**
     * Optimistic mode keeps single-node-like mods working while the snapshot is in flight.
     */
    @Test
    @DisplayName("unknown owner is allowed in optimistic mode")
    void unknownOwnerOptimistic() {
        MutationVerdict verdict = OwnershipPolicy.evaluateBlockMutation(true, true, true, SELF, null, true);
        assertTrue(verdict.isAllowed());
        assertEquals(OwnershipPolicy.REASON_UNKNOWN_OPTIMISTIC, verdict.reason());
    }

    /**
     * Strict mode refuses unassigned chunks so two nodes can never both write them.
     */
    @Test
    @DisplayName("unknown owner is rejected in strict mode")
    void unknownOwnerStrict() {
        MutationVerdict verdict = OwnershipPolicy.evaluateBlockMutation(true, true, true, SELF, null, false);
        assertTrue(verdict.isRejected());
        assertEquals(OwnershipPolicy.REASON_UNKNOWN_STRICT, verdict.reason());
    }

    /**
     * The happy path: loaded chunk, local owner.
     */
    @Test
    @DisplayName("loaded locally owned chunk is allowed")
    void localLoadedAllowed() {
        MutationVerdict verdict = OwnershipPolicy.evaluateBlockMutation(true, true, true, SELF, SELF, false);
        assertTrue(verdict.isAllowed());
        assertEquals(OwnershipPolicy.REASON_LOCAL_OWNER, verdict.reason());
    }

    /**
     * Players are node-local by definition; relaying a player entity would duplicate it.
     */
    @Test
    @DisplayName("player entities are never relayed")
    void playerEntityAllowed() {
        MutationVerdict verdict = OwnershipPolicy.evaluateEntityMutation(true, true, true, SELF, OTHER, false);
        assertTrue(verdict.isAllowed());
        assertEquals(OwnershipPolicy.REASON_PLAYER_ENTITY, verdict.reason());
    }

    /**
     * Non-player entities in foreign chunks are relayed.
     */
    @Test
    @DisplayName("foreign entity spawns are relayed")
    void foreignEntityRelayed() {
        MutationVerdict verdict = OwnershipPolicy.evaluateEntityMutation(true, true, false, SELF, OTHER, false);
        assertTrue(verdict.isRelayed());
    }

    /**
     * A relayed frame may only be applied by the node it was addressed to.
     */
    @Test
    @DisplayName("relay delivery is restricted to the addressed node")
    void relayDeliveryRestricted() {
        assertTrue(OwnershipPolicy.evaluateRelayDelivery(true, SELF, SELF).isAllowed());
        assertFalse(OwnershipPolicy.evaluateRelayDelivery(true, SELF, OTHER).isAllowed());
        assertTrue(OwnershipPolicy.evaluateRelayDelivery(true, SELF, null).isAllowed());
        assertTrue(OwnershipPolicy.evaluateRelayDelivery(false, SELF, OTHER).isAllowed());
    }
}
