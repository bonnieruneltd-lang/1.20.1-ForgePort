package dev.forgetalker.config;

import net.minecraftforge.common.ForgeConfigSpec;
import net.minecraftforge.eventbus.api.SubscribeEvent;
import net.minecraftforge.fml.event.config.ModConfigEvent;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Forge {@code SERVER} config plus the immutable {@link RuntimeSettings} snapshot derived from it.
 *
 * <h2>Operational purpose</h2>
 * Operators configure the cluster connection and the guard behaviour here. Every consumer
 * (transport, guards, dispatcher, command) reads {@link #active()} instead of the spec, so changing
 * a value never has to be coordinated with a running tick.
 *
 * <h2>Why SERVER type</h2>
 * Ownership and relaying are server-side concepts. A {@code SERVER} config is stored in
 * {@code <world>/serverconfig/forgetalker-server.toml} and is synced per world, which matches the
 * MultiPaper deployment model (one config per node world).
 *
 * <h2>Thread safety</h2>
 * {@link #SPEC} is only touched during config load/reload on the loading thread; {@link #ACTIVE} is
 * a {@code volatile} immutable record that any thread may read at any time.
 */
public final class ForgeTalkerConfig {

    /** Builder used by Forge to assemble and validate the TOML file. */
    public static final ForgeConfigSpec SPEC;

    private static final ForgeConfigSpec.BooleanValue CLUSTER_ENABLED;
    private static final ForgeConfigSpec.ConfigValue<String> MASTER_HOST;
    private static final ForgeConfigSpec.IntValue MASTER_PORT;
    private static final ForgeConfigSpec.ConfigValue<String> CLUSTER_SECRET;
    private static final ForgeConfigSpec.ConfigValue<String> NODE_ID_OVERRIDE;
    private static final ForgeConfigSpec.ConfigValue<String> PEER_BIND_HOST;
    private static final ForgeConfigSpec.IntValue PEER_BIND_PORT;
    private static final ForgeConfigSpec.IntValue RECONNECT_DELAY;
    private static final ForgeConfigSpec.IntValue HEARTBEAT_INTERVAL;
    private static final ForgeConfigSpec.IntValue HEARTBEAT_TIMEOUT;
    private static final ForgeConfigSpec.BooleanValue ENFORCE_BLOCK_GUARD;
    private static final ForgeConfigSpec.BooleanValue ENFORCE_ENTITY_GUARD;
    private static final ForgeConfigSpec.BooleanValue ASSUME_OWNERSHIP_WHEN_UNKNOWN;
    private static final ForgeConfigSpec.BooleanValue RELAY_MODDED_PAYLOADS;
    private static final ForgeConfigSpec.IntValue MAX_RELAYED_PAYLOAD_BYTES;
    private static final ForgeConfigSpec.IntValue PAYLOAD_HOP_LIMIT;
    private static final ForgeConfigSpec.IntValue DEFERRED_QUEUE_LIMIT;
    private static final ForgeConfigSpec.BooleanValue ANNOUNCE_CHUNK_INTEREST;
    private static final ForgeConfigSpec.BooleanValue DEBUG_LOGGING;

    /**
     * Snapshot read by hot paths. Starts in standalone mode so that a server which never loads the
     * config (client side, crash during startup) still behaves like vanilla.
     */
    private static volatile RuntimeSettings active = RuntimeSettings.standaloneDefaults();

    static {
        Pair<ForgeTalkerConfig, ForgeConfigSpec> pair = new ForgeConfigSpec.Builder()
                .configure(ForgeTalkerConfig::define);
        SPEC = pair.getRight();
    }

    private ForgeTalkerConfig() {
        // Holder class.
    }

    /**
     * Declares every config option. Kept package-private so only the static initialiser calls it.
     *
     * @param builder Forge config builder supplied by {@code configure(...)}
     * @return this holder instance (unused, required by the builder API)
     */
    private static ForgeTalkerConfig define(ForgeConfigSpec.Builder builder) {
        builder.comment("MultiPaper cluster connection. Leave clusterEnabled=false for a normal single server.")
                .push("cluster");
        CLUSTER_ENABLED = builder
                .comment("Connect to a MultiPaper master and enforce chunk ownership.",
                        "When false the mod is inert: every guard allows the mutation.")
                .define("clusterEnabled", false);
        MASTER_HOST = builder
                .comment("Hostname or IP of the MultiPaper master controller.")
                .define("masterHost", "127.0.0.1");
        MASTER_PORT = builder
                .comment("TCP port of the master controller.")
                .defineInRange("masterPort", 23545, 1, 65535);
        CLUSTER_SECRET = builder
                .comment("Shared secret used by the handshake. Never written to logs.")
                .define("clusterSecret", "");
        NODE_ID_OVERRIDE = builder
                .comment("Explicit node id. Leave empty to derive a stable id from the hostname and",
                        "persist it in config/forgetalker/node-id.txt.")
                .define("nodeId", "");
        PEER_BIND_HOST = builder
                .comment("Interface for the peer-to-peer listener.")
                .define("peerBindHost", "0.0.0.0");
        PEER_BIND_PORT = builder
                .comment("Port for the peer-to-peer listener. 0 disables direct peer links and",
                        "forces all traffic through the master.")
                .defineInRange("peerBindPort", 23546, 0, 65535);
        builder.pop();

        builder.comment("Session timing.").push("session");
        RECONNECT_DELAY = builder
                .comment("Base delay in seconds between reconnect attempts (exponential backoff up to 8x).")
                .defineInRange("reconnectDelaySeconds", 5, 1, 600);
        HEARTBEAT_INTERVAL = builder
                .comment("Seconds between heartbeat PING frames.")
                .defineInRange("heartbeatIntervalSeconds", 5, 1, 300);
        HEARTBEAT_TIMEOUT = builder
                .comment("Seconds of silence after which the session is dropped and re-established.")
                .defineInRange("heartbeatTimeoutSeconds", 20, 2, 3600);
        builder.pop();

        builder.comment("Ownership guards. Disable only for debugging.").push("guards");
        ENFORCE_BLOCK_GUARD = builder
                .comment("Intercept Level.setBlock and relay mutations for foreign chunks.")
                .define("enforceBlockMutationGuard", true);
        ENFORCE_ENTITY_GUARD = builder
                .comment("Intercept ServerLevel.addFreshEntity and relay spawns for foreign chunks.")
                .define("enforceEntityGuard", true);
        ASSUME_OWNERSHIP_WHEN_UNKNOWN = builder
                .comment("Treat chunks without a master assignment as local (optimistic).",
                        "true keeps single-node-like mods working while the snapshot is in flight.")
                .define("assumeOwnershipWhenUnknown", true);
        ANNOUNCE_CHUNK_INTEREST = builder
                .comment("Send CLAIM/RELEASE hints to the master when chunks load/unload.")
                .define("announceChunkInterest", true);
        builder.pop();

        builder.comment("Modded payload relaying.").push("payloads");
        RELAY_MODDED_PAYLOADS = builder
                .comment("Relay custom payloads sent by mods to the other nodes.")
                .define("relayModdedPayloads", true);
        MAX_RELAYED_PAYLOAD_BYTES = builder
                .comment("Payloads larger than this many bytes are dropped instead of relayed.")
                .defineInRange("maxRelayedPayloadBytes", 32768, 0, 1048576);
        PAYLOAD_HOP_LIMIT = builder
                .comment("Maximum hop count for a relayed payload. 0 means frames are never re-forwarded.")
                .defineInRange("relayPayloadHopLimit", 0, 0, 4);
        builder.pop();

        builder.comment("Memory limits.").push("limits");
        DEFERRED_QUEUE_LIMIT = builder
                .comment("Maximum deferred mutations buffered while a chunk is unloaded.")
                .defineInRange("deferredMutationQueueLimit", 4096, 1, 262144);
        DEBUG_LOGGING = builder
                .comment("Log every guarded/relayed mutation at DEBUG level.")
                .define("debugLogging", false);
        builder.pop();

        return new ForgeTalkerConfig();
    }

    /**
     * Rebuilds the immutable snapshot from the current spec values.
     *
     * @return the new snapshot, never {@code null}
     * @implNote Called on config load/reload only. Invalid combinations are clamped to safe values
     *         instead of throwing, because a failed reload would leave the server running with stale
     *         ownership settings.
     */
    private static RuntimeSettings bake() {
        int interval = HEARTBEAT_INTERVAL.get();
        int timeout = Math.max(HEARTBEAT_TIMEOUT.get(), interval + 1);
        return new RuntimeSettings(
                CLUSTER_ENABLED.get(),
                MASTER_HOST.get(),
                MASTER_PORT.get(),
                CLUSTER_SECRET.get(),
                NODE_ID_OVERRIDE.get(),
                PEER_BIND_HOST.get(),
                PEER_BIND_PORT.get(),
                RECONNECT_DELAY.get(),
                interval,
                timeout,
                ENFORCE_BLOCK_GUARD.get(),
                ENFORCE_ENTITY_GUARD.get(),
                ASSUME_OWNERSHIP_WHEN_UNKNOWN.get(),
                RELAY_MODDED_PAYLOADS.get(),
                MAX_RELAYED_PAYLOAD_BYTES.get(),
                PAYLOAD_HOP_LIMIT.get(),
                DEFERRED_QUEUE_LIMIT.get(),
                ANNOUNCE_CHUNK_INTEREST.get(),
                DEBUG_LOGGING.get());
    }

    /**
     * @return the current immutable settings snapshot, never {@code null}
     */
    public static RuntimeSettings active() {
        return active;
    }

    /**
     * Handles both initial load and reload of the mod config.
     *
     * @param event Forge config event, ignored unless it targets this mod's spec
     * @implNote Runs on the config loading thread, not the server thread. Only a volatile write is
     *         performed, so no world state is touched.
     */
    @SubscribeEvent
    public static void onConfigChanged(ModConfigEvent event) {
        if (event.getConfig().getSpec() != SPEC) {
            return;
        }
        active = bake();
    }
}
