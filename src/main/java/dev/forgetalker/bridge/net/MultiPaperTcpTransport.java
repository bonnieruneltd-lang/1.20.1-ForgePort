package dev.forgetalker.bridge.net;

import dev.forgetalker.bridge.ClusterMetrics;
import dev.forgetalker.bridge.ClusterTransport;
import dev.forgetalker.bridge.TransportListener;
import dev.forgetalker.bridge.TransportState;
import dev.forgetalker.ForgeTalker;
import dev.forgetalker.bridge.message.BridgeMessage;
import dev.forgetalker.bridge.message.BridgeMessageCodec;
import dev.forgetalker.bridge.message.BridgeProtocolException;
import dev.forgetalker.bridge.message.ControlMessages;
import dev.forgetalker.bridge.message.ControlMessages.Hello;
import dev.forgetalker.bridge.message.ControlMessages.HelloAck;
import dev.forgetalker.bridge.message.ControlMessages.PeerAdvertisement;
import dev.forgetalker.bridge.message.ControlMessages.PeerEndpoint;
import dev.forgetalker.bridge.message.ControlMessages.Ping;
import dev.forgetalker.bridge.message.ControlMessages.Pong;
import dev.forgetalker.bridge.message.ControlMessages.Reject;
import dev.forgetalker.bridge.message.MutationMessages.RoutedFrame;
import dev.forgetalker.cluster.ClusterNodeId;
import dev.forgetalker.config.RuntimeSettings;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Netty implementation of the MultiPaper bridge transport.
 *
 * <h2>Operational purpose</h2>
 * Maintains two kinds of connections:
 * <ul>
 *   <li><b>Master link</b> - one outbound TCP connection to the MultiPaper master. It carries the
 *       handshake, the ownership snapshot/deltas, heartbeats and - when a peer cannot be reached
 *       directly - routed frames.</li>
 *   <li><b>Peer links</b> - direct TCP connections between nodes (outbound dial plus an inbound
 *       listener). They carry block mutations, entity spawns and relayed mod payloads, which are
 *       latency sensitive and would otherwise all funnel through the master.</li>
 * </ul>
 *
 * <h2>Threading model</h2>
 * All socket work happens on a single Netty {@link NioEventLoopGroup} thread named
 * {@code ForgeTalker-Netty}. {@link #send(BridgeMessage)} may be called from the server thread and
 * only touches thread-safe structures ({@link ConcurrentLinkedQueue}, {@code volatile} fields), so
 * it never blocks a tick. Decoded frames are handed to {@link TransportListener#onMessage} on the
 * Netty thread; the production listener ({@code PeerMessageDispatcher}) immediately re-schedules
 * world work onto the server thread.
 *
 * <h2>Failure model</h2>
 * Dropping the master link is not fatal: the transport retries with exponential backoff. While the
 * link is down, {@link #state()} is not {@link TransportState#authoritative()}, so the guards fall
 * back to standalone behaviour and the server keeps running. Frames produced in that window are
 * buffered (bounded) and flushed after the next successful handshake.
 *
 * <h2>Security</h2>
 * The handshake carries the shared cluster secret and is compared in constant time. The transport
 * never logs the secret. There is no TLS: run the cluster on a private network or tunnel it, as
 * documented in {@code docs/PROTOCOL.md}.
 */
public final class MultiPaperTcpTransport implements ClusterTransport {

    private static final Logger LOGGER = LogManager.getLogger("ForgeTalker/Transport");

    /** Maximum number of frames buffered while the master link is down. */
    private static final int MAX_PENDING_FRAMES = 4096;

    /** Netty attribute holding the node id associated with a peer channel. */
    private static final AttributeKey<String> PEER_NODE = AttributeKey.valueOf("forgetalker.peerNode");

    private final RuntimeSettings settings;
    private final ClusterNodeId localNode;
    private final ClusterMetrics metrics;
    private final List<TransportListener> listeners = new CopyOnWriteArrayList<>();
    private final Queue<BridgeMessage> pending = new ConcurrentLinkedQueue<>();
    private final Map<String, Channel> peerChannels = new ConcurrentHashMap<>();
    private final AtomicLong pingNonce = new AtomicLong();
    private final AtomicInteger reconnectAttempt = new AtomicInteger();

    private volatile TransportState state = TransportState.DISCONNECTED;
    private volatile EventLoopGroup group;
    private volatile Channel masterChannel;
    private volatile Channel peerListenerChannel;
    private volatile ScheduledFuture<?> heartbeatTask;
    private volatile ScheduledFuture<?> reconnectTask;
    private volatile long lastPongMillis;
    private volatile boolean running;
    private volatile String lastDetail = "not started";

    /** Last inbound frame timestamp from the master, used by the heartbeat watchdog. */
    private volatile long lastMasterActivityMillis;

    /** Number of peers advertised by the master, used only for diagnostics. */
    private volatile int advertisedPeerCount;

    /**
     * @param settings  connection settings, never {@code null}
     * @param localNode this node's stable id, never {@code null}
     * @param metrics   counters to update, never {@code null}
     */
    public MultiPaperTcpTransport(RuntimeSettings settings, ClusterNodeId localNode, ClusterMetrics metrics) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.localNode = Objects.requireNonNull(localNode, "localNode");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    /**
     * Starts the event loop, the peer listener and the first master connection attempt.
     *
     * <h2>Threading</h2>
     * Called from the server thread during {@code ServerStartedEvent}. Returns immediately; all
     * socket work is asynchronous.
     */
    @Override
    public void start() {
        if (running) {
            return;
        }
        running = true;
        group = new NioEventLoopGroup(1, new DefaultThreadFactory("ForgeTalker-Netty", true));
        if (settings.peerBindPort() > 0) {
            bindPeerListener();
        }
        scheduleConnect(0);
    }

    /**
     * Stops the transport and shuts the Netty group down.
     *
     * @param reason logged and sent to peers as a GOODBYE frame, never {@code null}
     */
    @Override
    public void stop(String reason) {
        if (!running) {
            setState(TransportState.STOPPED, reason);
            return;
        }
        running = false;
        cancel(reconnectTask);
        cancel(heartbeatTask);
        BridgeMessage goodbye = new ControlMessages.Goodbye(reason);
        Channel master = masterChannel;
        if (master != null && master.isActive()) {
            master.writeAndFlush(goodbye);
        }
        for (Channel peer : peerChannels.values()) {
            if (peer.isActive()) {
                peer.writeAndFlush(goodbye);
            }
            peer.close();
        }
        peerChannels.clear();
        if (master != null) {
            master.close();
        }
        Channel listener = peerListenerChannel;
        if (listener != null) {
            listener.close();
        }
        EventLoopGroup loop = group;
        if (loop != null) {
            loop.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        }
        setState(TransportState.STOPPED, reason);
    }

    /**
     * Broadcasts a frame: written to the master link and to every live peer link.
     *
     * @param message frame to send, never {@code null}
     * @implNote Called from the server thread. If the session is not authoritative yet the frame is
     *         buffered and flushed after the handshake; if the buffer is full the oldest frame is
     *         dropped (ownership/event frames are periodically resent by their producers).
     */
    @Override
    public void send(BridgeMessage message) {
        Objects.requireNonNull(message, "message");
        if (!isSessionUsable()) {
            buffer(message);
            return;
        }
        Channel master = masterChannel;
        if (master != null && master.isActive()) {
            master.writeAndFlush(message);
        } else {
            buffer(message);
        }
        for (Channel peer : peerChannels.values()) {
            if (peer.isActive()) {
                peer.writeAndFlush(message);
            }
        }
    }

    /**
     * Sends a frame to one node, preferring a direct peer link and falling back to the master relay.
     *
     * @param target  destination node, never {@code null}
     * @param message frame to send, never {@code null}
     */
    @Override
    public void sendTo(ClusterNodeId target, BridgeMessage message) {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(message, "message");
        if (target.isSameNode(localNode)) {
            // Loopback: the caller wants this node to handle its own frame (e.g. an event that must
            // also be observed locally). Delivering through listeners keeps one code path.
            for (TransportListener listener : listeners) {
                listener.onMessage(message);
            }
            return;
        }
        Channel peer = peerChannels.get(target.value());
        if (peer != null && peer.isActive()) {
            peer.writeAndFlush(message);
            metrics.recordOutboundFrame();
            return;
        }
        Channel master = masterChannel;
        if (master != null && master.isActive()) {
            master.writeAndFlush(new RoutedFrame(target.value(), message));
            metrics.recordOutboundFrame();
        } else {
            buffer(message);
        }
    }

    /**
     * @return the current state, never {@code null}
     */
    @Override
    public TransportState state() {
        return state;
    }

    /**
     * @return a diagnostic line: state, master target, peer count and pending depth
     */
    @Override
    public String describe() {
        return state + " master=" + settings.masterHost() + ":" + settings.masterPort()
                + " peers=" + peerChannels.size() + " pending=" + pending.size()
                + " peerListener=" + (settings.peerBindPort() > 0
                    ? settings.peerBindHost() + ":" + settings.peerBindPort() : "disabled")
                + " last=\"" + lastDetail + "\"";
    }

    /**
     * @param listener listener to notify, never {@code null}
     */
    @Override
    public void addListener(TransportListener listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * @return the number of live peer links
     */
    @Override
    public int peerCount() {
        return peerChannels.size();
    }

    /**
     * @return the ids of nodes with a live direct link
     */
    @Override
    public java.util.Set<String> peerNodeIds() {
        return java.util.Set.copyOf(peerChannels.keySet());
    }

    /**
     * @return {@code true} while a cluster session is authoritative
     */
    private boolean isSessionUsable() {
        return state.authoritative() && masterChannel != null && masterChannel.isActive();
    }

    /**
     * Buffers a frame while the master link is unavailable.
     *
     * @param message frame to buffer, never {@code null}
     */
    private void buffer(BridgeMessage message) {
        while (pending.size() >= MAX_PENDING_FRAMES) {
            pending.poll();
        }
        pending.add(message);
    }

    /**
     * Flushes the buffered frames in order. Called once the handshake completed.
     */
    private void flushPending() {
        BridgeMessage message;
        while ((message = pending.poll()) != null) {
            send(message);
        }
    }

    /**
     * Schedules the next master connection attempt.
     *
     * @param delaySeconds delay in seconds; 0 connects immediately
     */
    private void scheduleConnect(int delaySeconds) {
        EventLoopGroup loop = group;
        if (!running || loop == null) {
            return;
        }
        cancel(reconnectTask);
        reconnectTask = loop.next().schedule(this::connect, delaySeconds, TimeUnit.SECONDS);
    }

    /**
     * Opens the outbound master connection and installs the pipeline.
     */
    private void connect() {
        EventLoopGroup loop = group;
        if (!running || loop == null) {
            return;
        }
        metrics.recordReconnectAttempt();
        setState(TransportState.CONNECTING,
                "dial " + settings.masterHost() + ":" + settings.masterPort());
        Bootstrap bootstrap = new Bootstrap()
                .group(loop)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    /**
                     * Installs framing, JSON codec and the handshake handler for the master link.
                     *
                     * @param channel freshly accepted/created socket channel
                     */
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        installPipeline(channel, false);
                    }
                });
        ChannelFuture future = bootstrap.connect(settings.masterHost(), settings.masterPort());
        future.addListener(result -> {
            if (!result.isSuccess()) {
                onMasterFailure(result.cause());
            }
        });
    }

    /**
     * Handles a failed connection attempt by logging and scheduling a retry with backoff.
     *
     * @param cause failure cause, may be {@code null}
     */
    private void onMasterFailure(Throwable cause) {
        int attempt = reconnectAttempt.incrementAndGet();
        setState(TransportState.DISCONNECTED, "connect failed: " + describeCause(cause));
        int delay = Math.min(settings.reconnectDelaySeconds() * (1 << Math.min(attempt - 1, 3)),
                settings.reconnectDelaySeconds() * 8);
        LOGGER.warn("Could not reach MultiPaper master at {}:{} ({}); retrying in {}s",
                settings.masterHost(), settings.masterPort(), describeCause(cause), delay);
        scheduleConnect(delay);
    }

    /**
     * Binds the peer-to-peer listener so other nodes can push data-plane frames directly.
     *
     * <h2>Threading</h2>
     * Called from {@link #start()} on the server thread; the bind itself is asynchronous.
     */
    private void bindPeerListener() {
        EventLoopGroup loop = group;
        if (loop == null) {
            return;
        }
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(loop)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 16)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    /**
                     * Installs the shared pipeline on an accepted peer connection.
                     *
                     * @param channel accepted socket channel
                     */
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        installPipeline(channel, true);
                    }
                });
        bootstrap.bind(settings.peerBindHost(), settings.peerBindPort()).addListener(result -> {
            if (result.isSuccess()) {
                peerListenerChannel = ((ChannelFuture) result).channel();
                LOGGER.info("Forge Talker peer listener bound to {}:{}",
                        settings.peerBindHost(), settings.peerBindPort());
            } else {
                LOGGER.warn("Could not bind peer listener to {}:{} - direct peer links disabled: {}",
                        settings.peerBindHost(), settings.peerBindPort(), describeCause(result.cause()));
            }
        });
    }

    /**
     * Dials a peer advertised by the master.
     *
     * @param endpoint peer endpoint, never {@code null}
     */
    private void dialPeer(PeerEndpoint endpoint) {
        EventLoopGroup loop = group;
        if (!running || loop == null) {
            return;
        }
        Bootstrap bootstrap = new Bootstrap()
                .group(loop)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    /**
                     * Installs the pipeline and tags the channel with the peer id.
                     *
                     * @param channel outbound peer socket
                     */
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.attr(PEER_NODE).set(endpoint.nodeId());
                        installPipeline(channel, true);
                    }
                });
        bootstrap.connect(endpoint.host(), endpoint.port()).addListener(result -> {
            if (result.isSuccess()) {
                Channel channel = ((ChannelFuture) result).channel();
                peerChannels.put(endpoint.nodeId(), channel);
                LOGGER.info("Direct peer link established to {}", endpoint);
            } else {
                LOGGER.debug("Direct peer link to {} failed ({}); frames will be routed via the master",
                        endpoint, describeCause(result.cause()));
            }
        });
    }

    /**
     * Ensures a direct link exists to every advertised peer.
     *
     * @param peers advertised peers, never {@code null}
     */
    private void ensurePeerLinks(List<PeerEndpoint> peers) {
        advertisedPeerCount = peers.size();
        for (PeerEndpoint endpoint : peers) {
            if (localNode.value().equals(endpoint.nodeId())) {
                continue;
            }
            Channel existing = peerChannels.get(endpoint.nodeId());
            if (existing != null && existing.isActive()) {
                continue;
            }
            dialPeer(endpoint);
        }
    }

    /**
     * Installs the shared Netty pipeline on a channel.
     *
     * @param channel  socket channel, never {@code null}
     * @param peerSide {@code true} for peer links, {@code false} for the master link
     * @implNote Pipeline order is critical: length decoding must precede JSON decoding, and the
     *         encoder is added before the application handler so outbound writes are framed.
     */
    private void installPipeline(SocketChannel channel, boolean peerSide) {
        channel.pipeline()
                .addLast(BridgeFrameCodec.LENGTH_HANDLER,
                        new LengthFieldBasedFrameDecoder(BridgeMessageCodec.MAX_FRAME_BYTES,
                                0, 4, 0, 4))
                .addLast(BridgeFrameCodec.DECODER_HANDLER, new BridgeFrameCodec.Decoder(this::onDecodeError))
                .addLast(BridgeFrameCodec.ENCODER_HANDLER, new BridgeFrameCodec.Encoder())
                .addLast("forgetalker-session", new BridgeSessionHandler(peerSide));
    }

    /**
     * Records and logs a decode failure without closing the session.
     *
     * @param error failure reported by the decoder, never {@code null}
     */
    private void onDecodeError(Throwable error) {
        metrics.recordDecodeError();
        if (error instanceof BridgeProtocolException protocol && protocol.isUnknownFrameType()) {
            LOGGER.debug("Ignoring frame from a newer protocol: {}", protocol.getMessage());
        } else {
            LOGGER.warn("Dropping malformed cluster frame: {}", error.toString());
        }
    }

    /**
     * Netty handler for one channel: performs the handshake and routes control frames.
     *
     * <h2>Threading</h2>
     * Confined to the channel's event loop; never touches world state.
     */
    private final class BridgeSessionHandler extends SimpleChannelInboundHandler<BridgeMessage> {

        private final boolean peerSide;

        /**
         * @param peerSide whether this channel belongs to the peer data plane
         */
        private BridgeSessionHandler(boolean peerSide) {
            this.peerSide = peerSide;
        }

        /**
         * Sends the HELLO handshake as soon as the socket is writable.
         *
         * @param ctx channel context
         */
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            Channel channel = ctx.channel();
            if (!peerSide) {
                masterChannel = channel;
            }
            channel.writeAndFlush(new Hello(localNode.value(), settings.clusterSecret(),
                    ForgeTalker.MOD_VERSION, ControlMessages.PROTOCOL_VERSION,
                    peerSide ? "peer" : "server", settings.peerBindPort()));
        }

        /**
         * Routes one decoded frame.
         *
         * @param ctx channel context
         * @param msg decoded frame, never {@code null}
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, BridgeMessage msg) {
            handleFrame(ctx.channel(), msg, peerSide);
        }

        /**
         * Cleans up when the socket closes.
         *
         * @param ctx channel context
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            handleDisconnect(ctx.channel(), peerSide);
        }

        /**
         * Logs and closes the channel on an I/O error.
         *
         * @param ctx   channel context
         * @param cause failure cause
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOGGER.debug("Cluster channel error on {}: {}", ctx.channel().remoteAddress(),
                    describeCause(cause));
            ctx.close();
        }
    }

    /**
     * Handles one decoded frame according to the protocol state machine.
     *
     * @param channel  channel the frame arrived on, never {@code null}
     * @param message  decoded frame, never {@code null}
     * @param peerSide whether the channel is a peer link
     */
    private void handleFrame(Channel channel, BridgeMessage message, boolean peerSide) {
        metrics.recordInboundFrame();
        if (channel == masterChannel) {
            lastMasterActivityMillis = System.currentTimeMillis();
        }
        if (message instanceof Hello hello) {
            handleHello(channel, hello);
            return;
        }
        if (message instanceof HelloAck ack) {
            handleHelloAck(channel, ack, peerSide);
            return;
        }
        if (message instanceof Reject reject) {
            handleReject(channel, reject);
            return;
        }
        if (message instanceof Ping ping) {
            channel.writeAndFlush(new Pong(ping.nonce(), ping.sentAtMillis()));
            return;
        }
        if (message instanceof Pong) {
            // Activity timestamp already refreshed above when the frame came from the master.
            return;
        }
        if (message instanceof PeerAdvertisement advertisement) {
            ensurePeerLinks(advertisement.peers());
            notifyMessage(advertisement);
            return;
        }
        if (message instanceof ControlMessages.Goodbye goodbye) {
            LOGGER.info("Cluster channel {} closed by remote: {}", channel.remoteAddress(),
                    goodbye.reason());
            channel.close();
            return;
        }
        notifyMessage(message);
    }

    /**
     * Validates an inbound HELLO (peer listener side) and answers with HELLO_ACK.
     *
     * @param channel channel that received the handshake
     * @param hello   handshake frame
     */
    private void handleHello(Channel channel, Hello hello) {
        if (!secretMatches(hello.clusterSecret())) {
            LOGGER.warn("Rejecting cluster handshake from {}: invalid cluster secret",
                    channel.remoteAddress());
            channel.writeAndFlush(new Reject("invalid cluster secret", false));
            channel.close();
            return;
        }
        if (hello.protocolVersion() != ControlMessages.PROTOCOL_VERSION) {
            LOGGER.warn("Rejecting cluster handshake from {}: protocol {} != {}",
                    channel.remoteAddress(), hello.protocolVersion(), ControlMessages.PROTOCOL_VERSION);
            channel.writeAndFlush(new Reject("protocol version mismatch", false));
            channel.close();
            return;
        }
        channel.attr(PEER_NODE).set(hello.nodeId());
        peerChannels.put(hello.nodeId(), channel);
        channel.writeAndFlush(new HelloAck(localNode.value(), "peer-link",
                ControlMessages.PROTOCOL_VERSION, 0L, List.of()));
        LOGGER.info("Accepted peer link from {} ({})", hello.nodeId(), channel.remoteAddress());
    }

    /**
     * Completes a handshake with the master or with a peer we dialed.
     *
     * @param channel  channel that received the acknowledgement
     * @param ack      acknowledgement frame
     * @param peerSide whether the channel is a peer link
     */
    private void handleHelloAck(Channel channel, HelloAck ack, boolean peerSide) {
        if (peerSide) {
            String peerId = channel.attr(PEER_NODE).get();
            if (peerId != null) {
                peerChannels.put(peerId, channel);
            }
            LOGGER.info("Peer handshake with {} complete (session {})", ack.masterNodeId(), ack.sessionId());
            notifyMessage(ack);
            return;
        }
        reconnectAttempt.set(0);
        lastMasterActivityMillis = System.currentTimeMillis();
        setState(TransportState.CONNECTED,
                "session " + ack.sessionId() + " master=" + ack.masterNodeId()
                        + " epoch=" + ack.ownershipEpoch());
        startHeartbeat();
        flushPending();
        ensurePeerLinks(ack.peers());
        notifyMessage(ack);
    }

    /**
     * Handles a master refusal. Non-retryable refusals stop reconnecting so a configuration error
     * cannot flood the master log.
     *
     * @param channel channel that received the refusal
     * @param reject  refusal frame
     */
    private void handleReject(Channel channel, Reject reject) {
        LOGGER.error("MultiPaper master refused the session: {} (retryable={})",
                reject.reason(), reject.retryable());
        setState(TransportState.DISCONNECTED, "rejected: " + reject.reason());
        if (!reject.retryable()) {
            running = false;
        }
        channel.close();
        if (running) {
            scheduleConnect(settings.reconnectDelaySeconds());
        }
    }

    /**
     * Cleans up bookkeeping after a channel closed and schedules a reconnect for the master link.
     *
     * @param channel  closed channel
     * @param peerSide whether the channel was a peer link
     */
    private void handleDisconnect(Channel channel, boolean peerSide) {
        if (peerSide) {
            String peerId = channel.attr(PEER_NODE).get();
            if (peerId != null) {
                peerChannels.remove(peerId, channel);
            }
            LOGGER.info("Peer link {} closed", peerId == null ? channel.remoteAddress() : peerId);
            return;
        }
        if (channel != masterChannel) {
            return;
        }
        masterChannel = null;
        cancel(heartbeatTask);
        setState(TransportState.DISCONNECTED, "master link closed");
        if (running) {
            int attempt = reconnectAttempt.incrementAndGet();
            int delay = Math.min(settings.reconnectDelaySeconds() * (1 << Math.min(attempt - 1, 3)),
                    settings.reconnectDelaySeconds() * 8);
            scheduleConnect(delay);
        }
    }

    /**
     * Starts (or restarts) the heartbeat task that detects a dead master link.
     *
     * <h2>Threading</h2>
     * Runs on the Netty event loop; only reads volatile fields and writes to the channel.
     */
    private void startHeartbeat() {
        cancel(heartbeatTask);
        EventLoopGroup loop = group;
        if (loop == null) {
            return;
        }
        long interval = settings.heartbeatIntervalSeconds();
        long timeoutMillis = settings.heartbeatTimeoutSeconds() * 1000L;
        heartbeatTask = loop.next().scheduleAtFixedRate(() -> {
            Channel master = masterChannel;
            if (!running || master == null || !master.isActive()) {
                return;
            }
            if (System.currentTimeMillis() - lastMasterActivityMillis > timeoutMillis) {
                LOGGER.warn("MultiPaper master silent for more than {}s; dropping the session",
                        settings.heartbeatTimeoutSeconds());
                master.close();
                return;
            }
            master.writeAndFlush(new Ping(pingNonce.incrementAndGet(), System.currentTimeMillis()));
        }, interval, interval, TimeUnit.SECONDS);
    }

    /**
     * Publishes a new state and notifies listeners.
     *
     * @param newState new state, never {@code null}
     * @param detail   human readable detail, never {@code null}
     */
    private void setState(TransportState newState, String detail) {
        TransportState previous = state;
        state = newState;
        lastDetail = detail;
        if (previous != newState) {
            LOGGER.info("Cluster transport state: {} -> {} ({})", previous, newState, detail);
        }
        for (TransportListener listener : listeners) {
            try {
                listener.onStateChanged(newState, detail);
            } catch (RuntimeException e) {
                LOGGER.warn("Transport listener failed on state change: {}", e.toString());
            }
        }
    }

    /**
     * Forwards an inbound frame to listeners, isolating listener failures.
     *
     * @param message frame to forward, never {@code null}
     */
    private void notifyMessage(BridgeMessage message) {
        for (TransportListener listener : listeners) {
            try {
                listener.onMessage(message);
            } catch (RuntimeException e) {
                LOGGER.warn("Transport listener failed on inbound frame {}: {}",
                        message.type(), e.toString());
            }
        }
    }

    /**
     * Compares the handshake secret in constant time.
     *
     * @param candidate secret sent by the remote node, may be {@code null}
     * @return {@code true} when it matches the configured secret
     * @implNote {@link MessageDigest#isEqual(byte[], byte[])} avoids leaking the match length through
     *         timing, which matters because the secret is the only authentication of the bus.
     */
    private boolean secretMatches(String candidate) {
        byte[] expected = settings.clusterSecret().getBytes(StandardCharsets.UTF_8);
        byte[] actual = (candidate == null ? "" : candidate).getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(expected, actual);
    }

    /**
     * Cancels a scheduled task when present.
     *
     * @param future task to cancel, may be {@code null}
     */
    private void cancel(ScheduledFuture<?> future) {
        if (future != null) {
            future.cancel(false);
        }
    }

    /**
     * @param cause failure cause, may be {@code null}
     * @return a short, log-friendly description
     */
    private static String describeCause(Throwable cause) {
        if (cause == null) {
            return "unknown";
        }
        String message = cause.getMessage();
        return cause.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }
}
