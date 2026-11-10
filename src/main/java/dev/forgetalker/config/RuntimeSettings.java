package dev.forgetalker.config;

/**
 * Immutable snapshot of the Forge Talker configuration, read on every hot path.
 *
 * <h2>Operational purpose</h2>
 * {@code ForgeConfigSpec} values live behind a thread-safe but relatively expensive lookup
 * (config value -> cached -> boxed). The mixin guards run for every block update, so they read this
 * flat record from a {@code volatile} field instead. A config reload swaps the whole record, which
 * makes the switch atomic for all readers.
 *
 * <h2>Thread safety</h2>
 * The record is immutable. It is published through {@code ForgeTalkerConfig#active()} (a volatile
 * field), so every thread sees either the old or the new snapshot, never a mixture.
 *
 * <h2>Cluster context</h2>
 * {@link #clusterEnabled()} is the master switch: when false, Forge Talker behaves exactly like a
 * vanilla Forge server and every ownership guard allows the mutation. This is deliberate so that a
 * misconfigured master can never brick a server.
 *
 * @param clusterEnabled             connect to a MultiPaper master and enforce ownership
 * @param masterHost                 master hostname or IP
 * @param masterPort                 master TCP port
 * @param clusterSecret              shared secret sent in the handshake (never logged)
 * @param nodeIdOverride             explicit node id; blank means "derive and persist"
 * @param peerBindHost               interface the peer-to-peer listener binds to
 * @param peerBindPort               port the peer-to-peer listener binds to, 0 disables it
 * @param reconnectDelaySeconds      base delay between reconnect attempts
 * @param heartbeatIntervalSeconds   interval between PING frames
 * @param heartbeatTimeoutSeconds    silence after which the session is considered dead
 * @param enforceBlockMutationGuard  install the {@code Level.setBlock} ownership guard
 * @param enforceEntityGuard         install the {@code ServerLevel.addFreshEntity} guard
 * @param assumeOwnershipWhenUnknown treat unassigned chunks as local (optimistic mode)
 * @param relayModdedPayloads        relay observed modded custom payloads to peers
 * @param maxRelayedPayloadBytes     payloads larger than this are dropped, not relayed
 * @param relayPayloadHopLimit       maximum hop count for relayed payloads (0 = never re-forward)
 * @param deferredMutationQueueLimit maximum number of deferred mutations held in memory
 * @param announceChunkInterest      send CLAIM/RELEASE hints to the master on chunk load/unload
 * @param debugLogging               emit per-mutation debug logs
 */
public record RuntimeSettings(boolean clusterEnabled,
                              String masterHost,
                              int masterPort,
                              String clusterSecret,
                              String nodeIdOverride,
                              String peerBindHost,
                              int peerBindPort,
                              int reconnectDelaySeconds,
                              int heartbeatIntervalSeconds,
                              int heartbeatTimeoutSeconds,
                              boolean enforceBlockMutationGuard,
                              boolean enforceEntityGuard,
                              boolean assumeOwnershipWhenUnknown,
                              boolean relayModdedPayloads,
                              int maxRelayedPayloadBytes,
                              int relayPayloadHopLimit,
                              int deferredMutationQueueLimit,
                              boolean announceChunkInterest,
                              boolean debugLogging) {

    /**
     * Validates the snapshot so a hostile or fat-fingered config cannot crash the transport later.
     *
     * @throws IllegalArgumentException if a port, timeout or limit is out of range
     */
    public RuntimeSettings {
        masterHost = masterHost == null || masterHost.isBlank() ? "127.0.0.1" : masterHost.trim();
        clusterSecret = clusterSecret == null ? "" : clusterSecret;
        nodeIdOverride = nodeIdOverride == null ? "" : nodeIdOverride.trim();
        peerBindHost = peerBindHost == null || peerBindHost.isBlank() ? "0.0.0.0" : peerBindHost.trim();
        if (masterPort < 1 || masterPort > 65535) {
            throw new IllegalArgumentException("masterPort out of range: " + masterPort);
        }
        if (peerBindPort < 0 || peerBindPort > 65535) {
            throw new IllegalArgumentException("peerBindPort out of range: " + peerBindPort);
        }
        if (reconnectDelaySeconds < 1) {
            throw new IllegalArgumentException("reconnectDelaySeconds must be >= 1");
        }
        if (heartbeatIntervalSeconds < 1) {
            throw new IllegalArgumentException("heartbeatIntervalSeconds must be >= 1");
        }
        if (heartbeatTimeoutSeconds <= heartbeatIntervalSeconds) {
            throw new IllegalArgumentException("heartbeatTimeoutSeconds must exceed heartbeatIntervalSeconds");
        }
        if (maxRelayedPayloadBytes < 0) {
            throw new IllegalArgumentException("maxRelayedPayloadBytes must be >= 0");
        }
        if (deferredMutationQueueLimit < 1) {
            throw new IllegalArgumentException("deferredMutationQueueLimit must be >= 1");
        }
    }

    /**
     * @return a settings snapshot with the cluster disabled, used before the config is loaded and
     *         in unit tests that exercise standalone behaviour
     */
    public static RuntimeSettings standaloneDefaults() {
        return new RuntimeSettings(false, "127.0.0.1", 23545, "", "", "0.0.0.0", 0,
                5, 5, 20, true, true, true, true, 32768, 0, 4096, true, false);
    }
}
