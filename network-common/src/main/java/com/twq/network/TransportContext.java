package com.twq.network;

import com.twq.network.client.TransportClient;
import com.twq.network.client.TransportClientFactory;
import com.twq.network.server.TransportChannelHandler;
import com.twq.network.server.TransportServer;
import io.netty.channel.socket.SocketChannel;

public class TransportContext {

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
        TransportChannelHandler channelHandler = createChannelHandler(channel);
        channel.pipeline()
                .addLast("handler", channelHandler);
        return channelHandler;
    }


    private TransportChannelHandler createChannelHandler(SocketChannel channel) {
        TransportClient client = new TransportClient(channel);

        return new TransportChannelHandler(client);
    }
}
