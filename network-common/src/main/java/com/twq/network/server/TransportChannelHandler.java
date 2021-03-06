package com.twq.network.server;

import com.twq.network.client.TransportClient;
import com.twq.network.client.TransportResponseHandler;
import com.twq.network.protocol.RequestMessage;
import com.twq.network.protocol.ResponseMessage;
import com.twq.network.util.NettyUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.twq.network.util.NettyUtils.getRemoteAddress;

/**
 * The single Transport-level Channel handler which is used for delegating requests to the
 * {@link TransportRequestHandler} and responses to the {@link TransportResponseHandler}.
 *
 * All channels created in the transport layer are bidirectional. When the Client initiates a Netty
 * Channel with a RequestMessage (which gets handled by the Server's RequestHandler), the Server
 * will produce a ResponseMessage (handled by the Client's ResponseHandler). However, the Server
 * also gets a handle on the same Channel, so it may then begin to send RequestMessages to the
 * Client.
 * This means that the Client also needs a RequestHandler and the Server needs a ResponseHandler,
 * for the Client's responses to the Server's requests.
 *
 * This class also handles timeouts from a {@link io.netty.handler.timeout.IdleStateHandler}.
 * We consider a connection timed out if there are outstanding fetch or RPC requests but no traffic
 * on the channel for at least `requestTimeoutMs`. Note that this is duplex traffic; we will not
 * timeout if the client is continuously sending but getting no responses, for simplicity.
 */
public class TransportChannelHandler
        extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(TransportChannelHandler.class);

    private final TransportClient client;

    private final TransportRequestHandler requestHandler;
    private final TransportResponseHandler responseHandler;

    private final long requestTimeoutNs;
    private final boolean closeIdleConnections;

    public TransportChannelHandler(
            TransportClient client,
            TransportRequestHandler requestHandler,
            TransportResponseHandler responseHandler,
            long requestTimeoutMs,
            boolean closeIdleConnections) {
        this.client = client;
        this.requestHandler = requestHandler;
        this.responseHandler = responseHandler;
        this.requestTimeoutNs = requestTimeoutMs * 1000L * 1000;
        this.closeIdleConnections = closeIdleConnections;
    }

    public TransportClient getClient() {
        return client;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.warn("Exception in connection from " + getRemoteAddress(ctx.channel()),
                cause);
        requestHandler.exceptionCaught(cause);
        responseHandler.exceptionCaught(cause);
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        try {
            requestHandler.channelActive();
        } catch (RuntimeException e) {
            logger.error("Exception from request handler while channel is active", e);
        }
        try {
            responseHandler.channelActive();
        } catch (RuntimeException e) {
            logger.error("Exception from response handler while channel is active", e);
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        try {
            requestHandler.channelInactive();
        } catch (RuntimeException e) {
            logger.error("Exception from request handler while channel is inactive", e);
        }
        try {
            responseHandler.channelInactive();
        } catch (RuntimeException e) {
            logger.error("Exception from response handler while channel is inactive", e);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object request) throws Exception {
        if (request instanceof RequestMessage) {
            // 请求消息
            requestHandler.handle((RequestMessage) request);
        } else if (request instanceof ResponseMessage) {
            // 相应详细
            responseHandler.handle((ResponseMessage) request);
        } else {
            ctx.fireChannelRead(request);
        }
    }

    /** Triggered based on events from an {@link io.netty.handler.timeout.IdleStateHandler}. */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            // See class comment for timeout semantics. In addition to ensuring we only timeout while
            // there are outstanding requests, we also do a secondary consistency check to ensure
            // there's no race between the idle timeout and incrementing the numOutstandingRequests
            // (see SPARK-7003).
            //
            // To avoid a race between TransportClientFactory.createClient() and this code which could
            // result in an inactive client being returned, this needs to run in a synchronized block.
            synchronized (this) {
                // 根据请求情况确定当前连接是否真的 timeout
                boolean isActuallyOverdue =
                        System.nanoTime() - responseHandler.getTimeOfLastRequestNs() > requestTimeoutNs;
                if (e.state() == IdleState.ALL_IDLE && isActuallyOverdue) {
                    String address = getRemoteAddress(ctx.channel());
                    logger.error("Connection to {} has been quiet for {} ms while there are outstanding " +
                            "requests. Assuming connection is dead; please adjust io.network.timeout if " +
                            "this is wrong.", address, requestTimeoutNs / 1000 / 1000);
                    client.timeOut();
                    ctx.close();
                } else if (closeIdleConnections) {
                    client.timeOut();
                    ctx.close();
                }

            }
        }
    }

    public TransportResponseHandler getResponseHandler() {
        return responseHandler;
    }
}
