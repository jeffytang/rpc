package com.twq.network.client;

import com.twq.network.TransportContext;
import com.twq.network.config.TransportConf;
import com.twq.network.server.TransportChannelHandler;
import com.twq.network.util.JavaUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;


public class TransportClientFactory implements Closeable {

    private final TransportConf conf;

    private static class ClientPool {
        TransportClient[] clients;
        Object[] locks;

        ClientPool(int size) {
            clients = new TransportClient[size];
            locks = new Object[size];
            for (int i = 0; i < size; i++)
                locks[i] = new Object();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

    private final TransportContext context;
    private final Class<? extends Channel> sorketChannel;
    private EventLoopGroup workerGroup;

    private ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;
    private final int numConnectionsPerPeer;
    private final Random random;

    public TransportClientFactory(TransportContext context) {
        this.context = context;
        this.sorketChannel = NioSocketChannel.class;
        this.workerGroup = new NioEventLoopGroup();

        this.conf = context.getConf();
        connectionPool = new ConcurrentHashMap<>();
        this.numConnectionsPerPeer = 5;
        random = new Random();
    }

    public TransportClient createClient(String remoteHost, int remotePort)
            throws IOException, InterruptedException{

        final InetSocketAddress unresolvedAddress
                = InetSocketAddress.createUnresolved(remoteHost, remotePort);

        ClientPool clientPool = connectionPool.get(unresolvedAddress);
        if (clientPool == null) {
            connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer));
            clientPool = connectionPool.get(unresolvedAddress);
        }

        int clientIndex = random.nextInt(numConnectionsPerPeer);
        TransportClient cachedClient = clientPool.clients[clientIndex];
        if (cachedClient != null && cachedClient.isActive()) {
            logger.trace("Returning cached connection to {}: {}",
                    cachedClient.getSocketAddress(), cachedClient);
            return cachedClient;
        }

        // 根据 DNS 解析的时间来改变日志级别
        final long preResolveHost = System.nanoTime();
        final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
        final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
        if (hostResolveTimeMs > 2000) {
            logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
        } else {
            logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
        }

        synchronized (clientPool.locks[clientIndex]) {
            cachedClient = clientPool.clients[clientIndex];
            if (cachedClient != null) {
                if (cachedClient.isActive()) {
                    logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
                    return cachedClient;
                } else {
                    logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
                }
            }
            clientPool.clients[clientIndex] = createClient(resolvedAddress);
            return clientPool.clients[clientIndex];
        }
    }

    private TransportClient createClient(InetSocketAddress address)
            throws IOException, InterruptedException {
        logger.debug("Creating new connection to {}", address);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(sorketChannel);

        AtomicReference<TransportClient> clientRef = new AtomicReference<>();

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel socketChannel) throws Exception {
                TransportChannelHandler channelHandler =
                        context.initializePipeline(socketChannel);
                clientRef.set(channelHandler.getClient());
            }
        });

        // Connect to the remote server
        ChannelFuture cf = bootstrap.connect(address);
        if (!cf.await(conf.connectionTimeoutMs())) {
            throw new IOException(
                    String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
        } else if (cf.cause() != null) {
            throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
        }
        TransportClient client = clientRef.get();
        assert client != null : "Channel future completed successfully with null client";

        return client;
    }

    /** Close all connections in the connection pool, and shutdown the worker thread pool. */
    @Override
    public void close() {
        // Go through all clients and close them if they are active.
        for (ClientPool clientPool : connectionPool.values()) {
            for (int i = 0; i < clientPool.clients.length; i++) {
                TransportClient client = clientPool.clients[i];
                if (client != null) {
                    clientPool.clients[i] = null;
                    JavaUtils.closeQuietly(client);
                }
            }
        }
        connectionPool.clear();

        if (workerGroup != null && !workerGroup.isShuttingDown()) {
            workerGroup.shutdownGracefully();
        }
    }
}
