package com.twq.network.client;

import com.twq.network.protocol.ResponseMessage;
import com.twq.network.protocol.RpcFailure;
import com.twq.network.protocol.RpcResponse;
import com.twq.network.server.MessageHandler;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.twq.network.util.NettyUtils.getRemoteAddress;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handler that processes server responses, in response to requests issued from a
 * [[TransportClient]]. It works by tracking the list of outstanding requests (and their callbacks).
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

    private final Channel channel;
    private final Map<Long, RpcResponseCallback> outstandingRpcs;

    public TransportResponseHandler(Channel channel) {
        this.channel = channel;
        this.outstandingRpcs = new ConcurrentHashMap<>();
    }

    public void addRpcRequest(long requestId, RpcResponseCallback callback) {
        outstandingRpcs.put(requestId, callback);
    }

    @Override
    public void handle(ResponseMessage message) throws Exception {
        if (message instanceof RpcResponse) {
            RpcResponse resp = (RpcResponse) message;
            RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
            if (listener == null) {
                logger.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
                        resp.requestId, getRemoteAddress(channel), resp.body().size());
            } else {
                outstandingRpcs.remove(resp.requestId);
                try {
                    listener.onSuccess(resp.body().nioByteBuffer());
                } finally {
                    resp.body().release();
                }
            }
        } else if (message instanceof RpcFailure) {
            RpcFailure resp = (RpcFailure) message;
            RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
            if (listener == null) {
                logger.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
                        resp.requestId, getRemoteAddress(channel), resp.errorString);
            } else {
                outstandingRpcs.remove(resp.requestId);
                listener.onFailure(new RuntimeException(resp.errorString));
            }
        }
    }

    private void failOutstandingRequests(Throwable cause) {
        for (Map.Entry<Long, RpcResponseCallback> entry : outstandingRpcs.entrySet()) {
            try {
                entry.getValue().onFailure(cause);
            } catch (Exception e) {
                logger.warn("RpcResponseCallback.onFailure throws exception", e);
            }
        }
        outstandingRpcs.clear();
    }

    @Override
    public void channelActive() {

    }

    @Override
    public void exceptionCaught(Throwable cause) {
        // TODO
    }

    @Override
    public void channelInactive() {
        // TODO
    }
}
