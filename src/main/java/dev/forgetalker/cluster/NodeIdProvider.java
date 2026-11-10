package dev.forgetalker.cluster;

import dev.forgetalker.config.RuntimeSettings;
import net.minecraftforge.fml.loading.FMLPaths;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.UUID;

/**
 * Resolves and persists the stable identity of this node.
 *
 * <h2>Operational purpose</h2>
 * The MultiPaper master keys chunk ownership by node id. If a node regenerated its id on every
 * restart, the master would treat it as a brand new node: old chunks would stay assigned to a
 * "ghost" node and never be released, eventually stalling ownership transfers. The id therefore
 * lives in {@code config/forgetalker/node-id.txt} and is only generated once.
 *
 * <h2>Order of precedence</h2>
 * <ol>
 *   <li>{@code cluster.nodeId} in the config, when non-blank (explicit operator override)</li>
 *   <li>the persisted file</li>
 *   <li>a freshly generated id, derived from the hostname plus a random suffix</li>
 * </ol>
 *
 * <h2>Thread safety</h2>
 * {@link #resolve(RuntimeSettings)} is called once during server bootstrap on the server thread.
 * The file I/O is synchronous and intentionally simple; a failure to persist is logged as a warning
 * and the generated id is kept for the current session.
 */
public final class NodeIdProvider {

    private static final Logger LOGGER = LogManager.getLogger("ForgeTalker/NodeId");

    /** Directory (relative to the Forge config directory) holding Forge Talker runtime state. */
    public static final String STATE_DIRECTORY = "forgetalker";

    /** File holding the persisted node id. */
    public static final String NODE_ID_FILE = "node-id.txt";

    private NodeIdProvider() {
        // Stateless utility.
    }

    /**
     * Resolves the node id for this server session.
     *
     * @param settings current settings snapshot, never {@code null}
     * @return a validated, stable node id, never {@code null}
     */
    public static ClusterNodeId resolve(RuntimeSettings settings) {
        String override = settings.nodeIdOverride();
        if (override != null && !override.isBlank()) {
            return ClusterNodeId.parse(override);
        }
        Path file = stateDirectory().resolve(NODE_ID_FILE);
        String persisted = readPersisted(file);
        if (persisted != null) {
            try {
                return ClusterNodeId.parse(persisted);
            } catch (IllegalArgumentException e) {
                LOGGER.warn("Ignoring invalid persisted node id in {}: {}", file, e.getMessage());
            }
        }
        ClusterNodeId generated = ClusterNodeId.parse(generate());
        persist(file, generated);
        return generated;
    }

    /**
     * @return {@code <config dir>/forgetalker}, created if missing
     * @implNote Called on the server thread during bootstrap only.
     */
    public static Path stateDirectory() {
        return FMLPaths.CONFIGDIR.get().resolve(STATE_DIRECTORY);
    }

    /**
     * Reads the persisted id, tolerating a missing or unreadable file.
     *
     * @param file node-id file path, never {@code null}
     * @return the trimmed file content, or {@code null} when absent/empty/unreadable
     */
    private static String readPersisted(Path file) {
        if (!Files.isRegularFile(file)) {
            return null;
        }
        try {
            String value = Files.readString(file, StandardCharsets.UTF_8).trim();
            return value.isEmpty() ? null : value;
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("Could not read node id from {}: {}", file, e.toString());
            return null;
        }
    }

    /**
     * Writes the generated id so the next restart reuses it.
     *
     * @param file node-id file path, never {@code null}
     * @param id   id to persist, never {@code null}
     */
    private static void persist(Path file, ClusterNodeId id) {
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, id.value() + System.lineSeparator(), StandardCharsets.UTF_8);
            LOGGER.info("Generated new cluster node id '{}' and stored it in {}", id, file);
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("Could not persist node id to {}; this session uses '{}'", file, id, e);
        }
    }

    /**
     * Builds a human-readable, DNS-safe id from the hostname plus a short random suffix.
     *
     * @return an id matching {@code [A-Za-z0-9_.-]{1,64}}
     * @implNote The random suffix prevents two containers that share a hostname from colliding.
     */
    private static String generate() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (IOException | RuntimeException e) {
            host = "node";
        }
        String sanitized = host.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_.-]", "-");
        if (sanitized.isBlank()) {
            sanitized = "node";
        }
        if (sanitized.length() > 40) {
            sanitized = sanitized.substring(0, 40);
        }
        String suffix = UUID.randomUUID().toString().substring(0, 6);
        return sanitized + "-" + suffix;
    }
}
