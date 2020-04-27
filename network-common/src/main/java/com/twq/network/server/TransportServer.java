package com.twq.network.server;

import com.twq.network.TransportContext;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * 服务端
 */
public class TransportServer implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportServer.class);

    private final TransportContext context;

    private ServerBootstrap bootstrap;
    private ChannelFuture channelFuture;
    private int port = -1;

    public TransportServer(
            TransportContext context,
            String hostToBind,
            int portToBind) {

        this.context = context;

        init(hostToBind, portToBind);
    }


    private void init(String hostToBind, int portToBind) {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class);

        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                logger.debug("New connection accepted for remote address {}.", ch.remoteAddress());
                // TODO 设置 handler
                context.initializePipeline(ch);
            }
        });

        InetSocketAddress address = hostToBind == null ?
                new InetSocketAddress(portToBind) : new InetSocketAddress(hostToBind, portToBind);
        channelFuture = bootstrap.bind(address);
        channelFuture.syncUninterruptibly();

        port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();

        logger.debug("server started on port: {}", port);
    }

    public int getPort() {
        if (port == -1)
            throw new IllegalStateException("Server not initialized");
        return port;
    }

    @Override
    public void close() throws IOException {
        if (channelFuture != null) {
            // close is a local operation and should finish within milliseconds; timeout just to be safe
            channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
            channelFuture = null;
        }

        if (bootstrap != null && bootstrap.config().group() != null) {
            bootstrap.config().group().shutdownGracefully();
        }

        if (bootstrap != null && bootstrap.config().childGroup() != null) {
            bootstrap.config().childGroup().shutdownGracefully();
        }

        bootstrap = null;
    }
}
