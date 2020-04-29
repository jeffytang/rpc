package com.twq.network.util;

import com.twq.network.protocol.TransportFrameDecoder;
import io.netty.channel.Channel;

public class NettyUtils {
    public static String getRemoteAddress(Channel channel) {
        if (channel != null && channel.remoteAddress() != null) {
            return channel.remoteAddress().toString();
        }
        return "<unknown remote>";
    }

    /**
     * Creates a LengthFieldBasedFrameDecoder where the first 8 bytes are the length of the frame.
     * This is used before all decoders.
     */
    public static TransportFrameDecoder createFrameDecoder() {
        return new TransportFrameDecoder();
    }
}
