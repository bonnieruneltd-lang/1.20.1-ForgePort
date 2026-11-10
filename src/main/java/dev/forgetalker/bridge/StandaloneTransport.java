package dev.forgetalker.bridge;

import dev.forgetalker.bridge.message.BridgeMessage;
import dev.forgetalker.cluster.ClusterNodeId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Inert transport used when {@code cluster.clusterEnabled=false}.
 *
 * <h2>Operational purpose</h2>
 * A server must never be forced to run a master just to load Forge mods. With this transport every
 * send is dropped, the state stays {@link TransportState#STANDALONE}, and therefore every ownership
 * guard allows the mutation: the server behaves exactly like vanilla Forge.
 *
 * <h2>Thread safety</h2>
 * State is a {@code volatile} field, listeners live in a {@link CopyOnWriteArrayList}; all methods
 * are safe from any thread. No thread or socket is ever created, so {@link #start()} costs nothing.
 */
public final class StandaloneTransport implements ClusterTransport {

    private static final Logger LOGGER = LogManager.getLogger("ForgeTalker/Transport");

    private final List<TransportListener> listeners = new CopyOnWriteArrayList<>();
    private volatile TransportState state = TransportState.STANDALONE;

    /**
     * No-op: logs once that the mod runs in standalone mode.
     */
    @Override
    public void start() {
        for (TransportListener listener : listeners) {
            listener.onStateChanged(TransportState.STANDALONE, "cluster disabled in config");
        }
        LOGGER.info("Cluster disabled (clusterEnabled=false); Forge Talker runs in standalone mode");
    }

    /**
     * @param reason ignored; the transport never had a session
     */
    @Override
    public void stop(String reason) {
        state = TransportState.STOPPED;
        for (TransportListener listener : listeners) {
            listener.onStateChanged(TransportState.STOPPED, reason);
        }
    }

    /**
     * Drops the frame. Standalone mode must never buffer cluster traffic.
     *
     * @param message frame that would have been sent, ignored
     */
    @Override
    public void send(BridgeMessage message) {
        // Intentionally empty: there is no peer to talk to.
    }

    /**
     * Drops the frame. Standalone mode has no peers.
     *
     * @param target  destination node, ignored
     * @param message frame that would have been sent, ignored
     */
    @Override
    public void sendTo(ClusterNodeId target, BridgeMessage message) {
        // Intentionally empty: there is no peer to talk to.
    }

    /**
     * @return always {@link TransportState#STANDALONE}
     */
    @Override
    public TransportState state() {
        return state;
    }

    /**
     * @return a constant description for {@code /forgetalker status}
     */
    @Override
    public String describe() {
        return "standalone (clusterEnabled=false)";
    }

    /**
     * @param listener listener to notify on start/stop, never {@code null}
     */
    @Override
    public void addListener(TransportListener listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
    }
}
