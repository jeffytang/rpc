package com.twq.network.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import com.sun.istack.internal.Nullable;
import com.twq.network.buffer.ManagedBuffer;
import com.twq.network.buffer.NioManagedBuffer;
import com.twq.network.protocol.OneWayMessage;
import com.twq.network.protocol.RpcRequest;
import com.twq.network.protocol.UploadStream;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.twq.network.util.NettyUtils.getRemoteAddress;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class TransportClient implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

    private Channel channel;
    private TransportResponseHandler handler;

    @Nullable
    private String clientId;

    private volatile boolean timedOut;

    public TransportClient(Channel channel, TransportResponseHandler handler) {
        this.channel = channel;
        this.handler = handler;
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

    /**
     * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
     * with the server's response or upon any failure.
     *
     * @param message The message to send.
     * @param callback Callback to handle the RPC's reply.
     * @return The RPC's id.
     */
    public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
        if (logger.isTraceEnabled()) {
            logger.trace("Sending RPC to {}", getRemoteAddress(channel));
        }

        long requestId = requestId();
        handler.addRpcRequest(requestId, callback);

        RpcChannelListener listener = new RpcChannelListener(requestId, callback);
        channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message)))
                .addListener(listener);

        return requestId;
    }

    public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
        final SettableFuture<ByteBuffer> result = SettableFuture.create();

        sendRpc(message, new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                try {
                    ByteBuffer copy = ByteBuffer.allocate(response.remaining());
                    copy.put(response);
                    // flip "copy" to make it readable
                    copy.flip();
                    result.set(copy);
                } catch (Throwable t) {
                    logger.warn("Error in responding PRC callback", t);
                    result.setException(t);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });

        try {
            return result.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Sends an opaque message to the RpcHandler on the server-side.
     * No reply is expected for the message,
     * and no delivery guarantees are made.
     *
     * @param message The message to send.
     */
    public void send(ByteBuffer message) {
        channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
    }

    /**
     * Send data to the remote end as a stream.  This differs from stream() in that this is a request
     * to *send* data to the remote end, not to receive it from the remote.
     *
     * @param meta meta data associated with the stream, which will be read completely on the
     *             receiving end before the stream itself.
     * @param data this will be streamed to the remote end to allow for transferring large amounts
     *             of data without reading into memory.
     * @param callback handles the reply -- onSuccess will only be called when both message and data
     *                 are received successfully.
     */
    public long uploadStream(
            ManagedBuffer meta,
            ManagedBuffer data,
            RpcResponseCallback callback) {
        if (logger.isTraceEnabled()) {
            logger.trace("Sending RPC to {}", getRemoteAddress(channel));
        }

        long requestId = requestId();
        handler.addRpcRequest(requestId, callback);

        RpcChannelListener listener = new RpcChannelListener(requestId, callback);
        channel.writeAndFlush(new UploadStream(requestId, meta, data)).addListener(listener);

        return requestId;
    }

    private static long requestId() {
        return Math.abs(UUID.randomUUID().getLeastSignificantBits());
    }

    @Override
    public void close() throws IOException {
        // TODO
    }

    private class StdChannelListener
            implements GenericFutureListener<Future<? super Void>> {
        final long startTime;
        final Object requestId;

        StdChannelListener(Object requestId) {
            this.startTime = System.currentTimeMillis();
            this.requestId = requestId;
        }

        @Override
        public void operationComplete(Future<? super Void> future) throws Exception {
            if (future.isSuccess()) {
                if (logger.isTraceEnabled()) {
                    long timeTaken = System.currentTimeMillis() - startTime;
                    logger.trace("Sending request {} to {} took {} ms", requestId,
                            getRemoteAddress(channel), timeTaken);
                }
            } else {
                String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
                        getRemoteAddress(channel), future.cause());
                logger.error(errorMsg, future.cause());
                channel.close();
                try {
                    handleFailure(errorMsg, future.cause());
                } catch (Exception e) {
                    logger.error("Uncaught exception in RPC response callback handler!", e);
                }
            }
        }

        void handleFailure(String errorMsg, Throwable cause) throws Exception {}
    }

    private class RpcChannelListener extends StdChannelListener {
        final long rpcRequestId;
        final RpcResponseCallback callback;

        RpcChannelListener(long rpcRequestId, RpcResponseCallback callback) {
            super("RPC " + rpcRequestId);
            this.rpcRequestId = rpcRequestId;
            this.callback = callback;
        }

        @Override
        void handleFailure(String errorMsg, Throwable cause) {
            handler.removeRpcRequest(rpcRequestId);
            callback.onFailure(new IOException(errorMsg, cause));
        }
    }
}
