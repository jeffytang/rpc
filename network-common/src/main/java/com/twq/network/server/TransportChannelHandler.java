package com.twq.network.server;

import com.twq.network.client.TransportClient;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransportChannelHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(TransportChannelHandler.class);

    private final TransportClient client;

    public TransportChannelHandler(TransportClient client) {
        this.client = client;
    }

    public TransportClient getClient() {
        return client;
    }
}
