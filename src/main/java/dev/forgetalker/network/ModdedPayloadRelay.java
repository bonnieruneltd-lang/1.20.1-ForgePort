package dev.forgetalker.network;

import dev.forgetalker.ForgeTalkerRuntime;
import dev.forgetalker.bridge.message.MutationMessages.PayloadRelay;
import dev.forgetalker.config.ForgeTalkerConfig;
import dev.forgetalker.config.RuntimeSettings;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerPlayer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

/**
 * Observes custom payloads sent by other mods and forwards them onto the cluster bus.
 *
 * <h2>Operational purpose</h2>
 * A Forge mod registers a {@code SimpleChannel} and expects its serverbound packets to be handled by
 * the node the player is connected to. On a MultiPaper cluster the authoritative state for the
 * player's chunk may live on a <em>different</em> node, so the owning node never sees the payload.
 * Relaying it cluster-wide fixes mods whose handlers mutate world state at the player's position
 * (crates, land claims, custom containers) without patching those mods.
 *
 * <h2>Which payloads are relayed</h2>
 * Everything except Minecraft's, Forge's and Forge Talker's own channels (see {@link #IGNORED_NAMESPACES}).
 * Vanilla/Forge channels are high frequency (brand, client settings) and must never hit the bus.
 *
 * <h2>Thread safety</h2>
 * Called from the Netty thread by {@code ServerGamePacketListenerImplMixin}, so this method must not
 * block and must not touch world state. It only reads immutable settings, copies bytes and enqueues a
 * frame on the transport.
 *
 * <h2>Loop prevention</h2>
 * Relayed frames are dispatched to local players by {@code RelayedPayloadDispatcher} through
 * {@code NetworkHooks}, which does <em>not</em> re-enter this observer (the observer only sees packets
 * that arrive from a socket). In addition the frame carries a hop count and
 * {@code relayPayloadHopLimit} defaults to 0, so a peer never re-forwards a relayed payload.
 */
public final class ModdedPayloadRelay {

    private static final Logger LOGGER = LogManager.getLogger("ForgeTalker/PayloadRelay");

    /** Namespaces that are never relayed: vanilla, Forge, FML and Forge Talker itself. */
    public static final Set<String> IGNORED_NAMESPACES =
            Set.of("minecraft", "forge", "fml", "fmlplayernetwork", "forgetalker");

    private ModdedPayloadRelay() {
        // Stateless utility.
    }

    /**
     * Inspects one inbound modded payload and forwards it to the cluster when eligible.
     *
     * @param sender  player whose connection delivered the payload, may be {@code null} (login phase)
     * @param channel payload channel, never {@code null}
     * @param data    raw payload bytes; not retained, never {@code null}
     * @return {@code true} when the payload was forwarded to the cluster
     */
    public static boolean observeInboundCustomPayload(ServerPlayer sender,
                                                      ResourceLocation channel,
                                                      byte[] data) {
        if (sender == null || channel == null || data == null) {
            return false;
        }
        RuntimeSettings settings = ForgeTalkerConfig.active();
        if (!settings.relayModdedPayloads()) {
            return false;
        }
        if (IGNORED_NAMESPACES.contains(channel.getNamespace())) {
            return false;
        }
        if (data.length > settings.maxRelayedPayloadBytes()) {
            LOGGER.warn("Dropping payload on channel {} ({} bytes) above the {} byte relay limit",
                    channel, data.length, settings.maxRelayedPayloadBytes());
            return false;
        }
        ForgeTalkerRuntime runtime = ForgeTalkerRuntime.get();
        if (runtime == null || !runtime.bridge().isClusterActive()) {
            return false;
        }
        runtime.bridge().broadcastPayload(new PayloadRelay(
                sender.getUUID(), channel.toString(), data, 0, runtime.nodeId().value()));
        if (settings.debugLogging()) {
            LOGGER.debug("Relayed {} byte payload on {} from {}", data.length, channel,
                    sender.getGameProfile().getName());
        }
        return true;
    }
}
