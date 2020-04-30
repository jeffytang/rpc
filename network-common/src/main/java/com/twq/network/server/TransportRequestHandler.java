package com.twq.network.server;

import com.google.common.base.Throwables;
import com.twq.network.buffer.ManagedBuffer;
import com.twq.network.buffer.NioManagedBuffer;
import com.twq.network.client.RpcResponseCallback;
import com.twq.network.client.StreamCallbackWithID;
import com.twq.network.client.StreamInterceptor;
import com.twq.network.client.TransportClient;
import com.twq.network.protocol.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import static com.twq.network.util.NettyUtils.getRemoteAddress;

/**
 * A handler that processes requests from clients and writes chunk data back. Each handler is
 * attached to a single Netty channel, and keeps track of which streams have been fetched via this
 * channel, in order to clean them up if the channel is terminated (see #channelUnregistered).
 *
 * The messages should have been processed by the pipeline setup by {@link TransportServer}.
 */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {
    private static final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);

    /** The Netty channel that this handler is associated with. */
    private final Channel channel;
    /** Client on the same channel allowing us to talk back to the requester. */
    private final TransportClient reverseClient;

    /** Handles all RPC messages. */
    private final RpcHandler rpcHandler;

    /** Returns each chunk part of a stream. */
    private final StreamManager streamManager;

    /** The max number of chunks being transferred and not finished yet. */
    private final long maxChunksBeingTransferred;

    public TransportRequestHandler(
            Channel channel,
            TransportClient reverseClient,
            RpcHandler rpcHandler,
            long maxChunksBeingTransferred) {
        this.channel = channel;
        this.reverseClient = reverseClient;
        this.rpcHandler = rpcHandler;
        this.streamManager = rpcHandler.getStreamManager();
        this.maxChunksBeingTransferred = maxChunksBeingTransferred;
    }
    @Override
    public void handle(RequestMessage message) throws Exception {
        if (message instanceof RpcRequest) {
            processRpcRequest((RpcRequest) message);
        } else if (message instanceof OneWayMessage) {
            processOneWayMessage((OneWayMessage) message);
        } else if (message instanceof UploadStream) {
            processStreamUpload((UploadStream) message);
        } else if (message instanceof StreamRequest) {
            processStreamRequest((StreamRequest) message);
        } else if (message instanceof ChunkFetchRequest) {
            processFetchRequest((ChunkFetchRequest) message);
        }
    }

    private void processFetchRequest(final ChunkFetchRequest req) {
        if (logger.isTraceEnabled()) {
            logger.trace("Received req from {} to fetch block {}", getRemoteAddress(channel),
                    req.streamChunkId);
        }
        long chunksBeingTransferred = streamManager.chunksBeingTransferred();
        if (chunksBeingTransferred >= maxChunksBeingTransferred) {
            logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
                    chunksBeingTransferred, maxChunksBeingTransferred);
            channel.close();
            return;
        }
        ManagedBuffer buf;
        try {
            streamManager.checkAuthorization(reverseClient, req.streamChunkId.streamId);
            // 先拿到 ManagedBuffer，对应的数据是在 MessageEncoder 中通过 channel 零拷贝到 socket 缓冲区
            // 请见 MessageEncoder
            buf = streamManager.getChunk(req.streamChunkId.streamId, req.streamChunkId.chunkIndex);
        } catch (Exception e) {
            logger.error(String.format("Error opening block %s for request from %s",
                    req.streamChunkId, getRemoteAddress(channel)), e);
            respond(new ChunkFetchFailure(req.streamChunkId, Throwables.getStackTraceAsString(e)));
            return;
        }

        streamManager.chunkBeingSent(req.streamChunkId.streamId);
        respond(new ChunkFetchSuccess(req.streamChunkId, buf)).addListener(future -> {
            streamManager.chunkSent(req.streamChunkId.streamId);
        });
    }

    private void processStreamRequest(final StreamRequest req) {
        if (logger.isTraceEnabled()) {
            logger.trace("Received req from {} to fetch stream {}", getRemoteAddress(channel),
                    req.streamId);
        }

        long chunksBeingTransferred = streamManager.chunksBeingTransferred();
        if (chunksBeingTransferred >= maxChunksBeingTransferred) {
            logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
                    chunksBeingTransferred, maxChunksBeingTransferred);
            channel.close();
            return;
        }

        ManagedBuffer buf;
        try {
            // 先拿到 ManagedBuffer，对应的数据是在 MessageEncoder 中通过 channel 零拷贝到 socket 缓冲区
            // 请见 MessageEncoder
            buf = streamManager.openStream(req.streamId);
        } catch (Exception e) {
            logger.error(String.format(
                    "Error opening stream %s for request from %s", req.streamId, getRemoteAddress(channel)), e);
            respond(new StreamFailure(req.streamId, Throwables.getStackTraceAsString(e)));
            return;
        }

        if (buf != null) {
            streamManager.streamBeingSent(req.streamId);
            respond(new StreamResponse(req.streamId, buf.size(), buf)).addListener(future -> {
                streamManager.streamSent(req.streamId);
            });
        } else {
            respond(new StreamFailure(req.streamId, String.format(
                    "Stream '%s' was not found.", req.streamId)));
        }
    }


    /**
     * Handle a request from the client to upload a stream of data.
     */
    private void processStreamUpload(final UploadStream req) {
        assert (req.body() == null);
        try {
            RpcResponseCallback callback = new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
                }

                @Override
                public void onFailure(Throwable e) {
                    respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
                }
            };
            TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
                    channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
            ByteBuffer meta = req.meta.nioByteBuffer();
            StreamCallbackWithID streamHandler = rpcHandler.receiveStream(reverseClient, meta, callback);
            if (streamHandler == null) {
                throw new NullPointerException("rpcHandler returned a null streamHandler");
            }
            StreamCallbackWithID wrappedCallback = new StreamCallbackWithID() {
                @Override
                public void onData(String streamId, ByteBuffer buf) throws IOException {
                    streamHandler.onData(streamId, buf);
                }

                @Override
                public void onComplete(String streamId) throws IOException {
                    try {
                        streamHandler.onComplete(streamId);
                        callback.onSuccess(ByteBuffer.allocate(0));
                    } catch (Exception ex) {
                        IOException ioExc = new IOException("Failure post-processing complete stream;" +
                                " failing this rpc and leaving channel active", ex);
                        callback.onFailure(ioExc);
                        streamHandler.onFailure(streamId, ioExc);
                    }
                }

                @Override
                public void onFailure(String streamId, Throwable cause) throws IOException {
                    callback.onFailure(new IOException("Destination failed while reading stream", cause));
                    streamHandler.onFailure(streamId, cause);
                }

                @Override
                public String getID() {
                    return streamHandler.getID();
                }
            };
            if (req.bodyByteCount > 0) {
                StreamInterceptor<RequestMessage> interceptor = new StreamInterceptor<>(
                        this, wrappedCallback.getID(), req.bodyByteCount, wrappedCallback);
                frameDecoder.setInterceptor(interceptor);
            } else {
                wrappedCallback.onComplete(wrappedCallback.getID());
            }
        } catch (Exception e) {
            logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
            respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
            // We choose to totally fail the channel, rather than trying to recover as we do in other
            // cases.  We don't know how many bytes of the stream the client has already sent for the
            // stream, it's not worth trying to recover.
            channel.pipeline().fireExceptionCaught(e);
        } finally {
            req.meta.release();
        }
    }

    private void processOneWayMessage(OneWayMessage req) {
        try {
            rpcHandler.receive(reverseClient, req.body().nioByteBuffer());
        } catch (Exception e) {
            logger.error("Error while invoking RpcHandler#receive() for one-way message.", e);
        } finally {
            req.body().release();
        }
    }

    private void processRpcRequest(final RpcRequest req) {
        try {
            rpcHandler.receive(reverseClient, req.body().nioByteBuffer(), new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
                }

                @Override
                public void onFailure(Throwable e) {
                    respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
                }
            });
        } catch (Exception e) {
            logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
        }
    }

    private ChannelFuture respond(Encodable result) {
        SocketAddress remoteAddress = channel.remoteAddress();
        return channel.writeAndFlush(result).addListener(future -> {
            if (future.isSuccess()) {
                logger.trace("Sent result {} to client {}", result, remoteAddress);
            } else {
                logger.error(String.format("Error sending result %s to %s; closing connection",
                        result, remoteAddress), future.cause());
                channel.close();
            }
        });
    }

    @Override
    public void channelActive() {
        rpcHandler.channelActive(reverseClient);
    }

    @Override
    public void exceptionCaught(Throwable cause) {
        rpcHandler.exceptionCaught(cause, reverseClient);
    }

    @Override
    public void channelInactive() {
        rpcHandler.channelInactive(reverseClient);
    }
}
