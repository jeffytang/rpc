package com.twq.network.client;

import com.google.common.base.Preconditions;
import com.sun.istack.internal.Nullable;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;

public class TransportClient implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

    private Channel channel;

    @Nullable
    private String clientId;

    private volatile boolean timedOut;

    public TransportClient(Channel channel) {
        this.channel = channel;
        timedOut = false;
    }

    public Channel getChannel() {
        return channel;
    }

    public boolean isActive() {
        return !timedOut && (channel.isOpen() || channel.isActive());
    }

    public SocketAddress getSocketAddress() {
        return channel.remoteAddress();
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        Preconditions.checkState(clientId == null, "Client ID has already been set.");
        this.clientId = clientId;
    }

    @Override
    public void close() throws IOException {

    }
}
