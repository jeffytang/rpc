package com.twq.network.client;

import com.twq.network.protocol.ResponseMessage;
import com.twq.network.server.MessageHandler;

public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
    @Override
    public void handle(ResponseMessage message) throws Exception {

    }

    @Override
    public void channelActive() {

    }

    @Override
    public void exceptionCaught(Throwable cause) {

    }

    @Override
    public void channelInactive() {

    }
}
