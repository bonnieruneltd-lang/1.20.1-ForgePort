package dev.forgetalker;

import dev.forgetalker.config.ForgeTalkerConfig;
import dev.forgetalker.network.ForgeTalkerNetwork;
import net.minecraftforge.eventbus.api.IEventBus;
import net.minecraftforge.fml.ModLoadingContext;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.config.ModConfig;
import net.minecraftforge.fml.javafmlmod.FMLJavaModLoadingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Forge entry point for Forge Talker.
 *
 * <h2>Operational purpose</h2>
 * Wires the mod into Forge: registers the {@code SERVER} config spec, subscribes the config reloader
 * and creates the {@code SimpleChannel}. All cluster logic lives in {@link ForgeTalkerRuntime} and is
 * started from {@code ServerStartedEvent}, because a mod constructor runs before a server exists.
 *
 * <h2>Side handling</h2>
 * The class loads on both sides. On a client it only registers the channel and the config; the
 * runtime stays null, so every guard is inert. This is deliberate: a client without a cluster must
 * still be able to join a cluster node.
 *
 * <h2>Thread safety</h2>
 * The constructor runs on the mod-loading thread. It only registers callbacks and never touches world
 * state, so it is safe regardless of which thread Forge uses.
 */
@Mod(ForgeTalker.MOD_ID)
public final class ForgeTalker {

    /** Mod id; must match {@code mod_id} in {@code gradle.properties} and {@code mods.toml}. */
    public static final String MOD_ID = "forgetalker";

    /**
     * Mod version reported in handshakes.
     *
     * <p>Keep in sync with {@code mod_version} in {@code gradle.properties}. It is duplicated as a
     * constant because the handshake runs on a Netty thread and reading the mod container there can
     * trigger class loading during shutdown.
     */
    public static final String MOD_VERSION = "1.0.0";

    private static final Logger LOGGER = LogManager.getLogger("ForgeTalker");

    /**
     * Registers config, config-reload listener and the network channel.
     *
     * <h2>Threading</h2>
     * Mod-construction thread. No world access, no I/O.
     */
    public ForgeTalker() {
        IEventBus modBus = FMLJavaModLoadingContext.get().getModEventBus();
        ModLoadingContext.get().registerConfig(ModConfig.Type.SERVER, ForgeTalkerConfig.SPEC);
        // ModConfigEvent is posted on the MOD bus in 1.20.1, not on the Forge bus.
        modBus.addListener(ForgeTalkerConfig::onConfigChanged);
        ForgeTalkerNetwork.register();
        LOGGER.info("Forge Talker {} loaded; waiting for the server to start before joining the cluster",
                MOD_VERSION);
    }
}
