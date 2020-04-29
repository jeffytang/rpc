package com.twq.network;

import com.twq.network.client.TransportClient;
import com.twq.network.client.TransportClientFactory;
import com.twq.network.protocol.MessageDecoder;
import com.twq.network.protocol.MessageEncoder;
import com.twq.network.server.TransportChannelHandler;
import com.twq.network.server.TransportServer;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransportContext {
    private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

    private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
    private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

    public TransportClientFactory createClientFactory() {
        return new TransportClientFactory(this);
    }

    public TransportServer createServer(int port) {
        return new TransportServer(this, null, port);
    }

    public TransportServer createServer(String host, int port) {
        return new TransportServer(this, host, port);
    }

    public TransportChannelHandler initializePipeline(SocketChannel channel) {

        try {
            TransportChannelHandler channelHandler = createChannelHandler(channel);
            channel.pipeline()
                    .addLast("encoder", ENCODER)
                    .addLast("decoder", DECODER)
                    .addLast("handler", channelHandler);
            return channelHandler;
        } catch (RuntimeException e) {
            logger.error("Error while initializing Netty pipeline", e);
            throw e;
        }

    }


    private TransportChannelHandler createChannelHandler(SocketChannel channel) {
        TransportClient client = new TransportClient(channel);

        return new TransportChannelHandler(client);
    }
}
