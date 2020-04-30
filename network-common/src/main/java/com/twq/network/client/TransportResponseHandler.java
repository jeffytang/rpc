package com.twq.network.client;

import com.google.common.annotations.VisibleForTesting;
import com.twq.network.protocol.*;
import com.twq.network.server.MessageHandler;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.twq.network.util.NettyUtils.getRemoteAddress;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

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
    private final Queue<Pair<String, StreamCallback>> streamCallbacks;
    private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

    /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent. */
    private final AtomicLong timeOfLastRequestNs;

    private volatile boolean streamActive;

    public TransportResponseHandler(Channel channel) {
        this.channel = channel;
        this.outstandingRpcs = new ConcurrentHashMap<>();
        this.streamCallbacks = new ConcurrentLinkedQueue<>();
        this.outstandingFetches = new ConcurrentHashMap<>();
        this.timeOfLastRequestNs = new AtomicLong(0);
    }

    public void removeFetchRequest(StreamChunkId streamChunkId) {
        outstandingFetches.remove(streamChunkId);
    }

    public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback) {
        updateTimeOfLastRequest();
        outstandingFetches.put(streamChunkId, callback);
    }

    public void addStreamCallback(String streamId, StreamCallback callback) {
        updateTimeOfLastRequest();
        streamCallbacks.offer(ImmutablePair.of(streamId, callback));
    }

    public void addRpcRequest(long requestId, RpcResponseCallback callback) {
        updateTimeOfLastRequest();
        outstandingRpcs.put(requestId, callback);
    }

    public void removeRpcRequest(long requestId) {
        outstandingRpcs.remove(requestId);
    }

    /** Returns the time in nanoseconds of when the last request was sent out. */
    public long getTimeOfLastRequestNs() {
        return timeOfLastRequestNs.get();
    }

    /** Updates the time of the last request to the current system time. */
    public void updateTimeOfLastRequest() {
        timeOfLastRequestNs.set(System.nanoTime());
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
        } else if (message instanceof StreamResponse) {
            StreamResponse resp = (StreamResponse) message;
            Pair<String, StreamCallback> entry = streamCallbacks.poll();
            if (entry != null) {
                StreamCallback callback = entry.getValue();
                if (resp.byteCount > 0) {
                    StreamInterceptor<ResponseMessage> interceptor = new StreamInterceptor<>(
                            this, resp.streamId, resp.byteCount, callback);
                    try {
                        TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
                                channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
                        frameDecoder.setInterceptor(interceptor);
                        streamActive = true;
                    } catch (Exception e) {
                        logger.error("Error installing stream handler.", e);
                        deactivateStream();
                    }
                } else {
                    try {
                        callback.onComplete(resp.streamId);
                    } catch (Exception e) {
                        logger.warn("Error in stream handler onComplete().", e);
                    }
                }
            } else {
                logger.error("Could not find callback for StreamResponse.");
            }
        } else if (message instanceof StreamFailure) {
            StreamFailure resp = (StreamFailure) message;
            Pair<String, StreamCallback> entry = streamCallbacks.poll();
            if (entry != null) {
                StreamCallback callback = entry.getValue();
                try {
                    callback.onFailure(resp.streamId, new RuntimeException(resp.error));
                } catch (IOException ioe) {
                    logger.warn("Error in stream failure handler.", ioe);
                }
            } else {
                logger.warn("Stream failure with unknown callback: {}", resp.error);
            }
        } else if (message instanceof ChunkFetchSuccess) {
            ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
            ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
            if (listener == null) {
                logger.warn("Ignoring response for block {} from {} since it is not outstanding",
                        resp.streamChunkId, getRemoteAddress(channel));
                resp.body().release();
            } else {
                outstandingFetches.remove(resp.streamChunkId);
                listener.onSuccess(resp.streamChunkId.chunkIndex, resp.body());
                resp.body().release();
            }
        } else if (message instanceof ChunkFetchFailure) {
            ChunkFetchFailure resp = (ChunkFetchFailure) message;
            ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
            if (listener == null) {
                logger.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
                        resp.streamChunkId, getRemoteAddress(channel), resp.errorString);
            } else {
                outstandingFetches.remove(resp.streamChunkId);
                listener.onFailure(resp.streamChunkId.chunkIndex, new ChunkFetchFailureException(
                        "Failure while fetching " + resp.streamChunkId + ": " + resp.errorString));
            }
        } else {
            throw new IllegalStateException("Unknown response type: " + message.type());
        }
    }

    @VisibleForTesting
    public void deactivateStream() {
        streamActive = false;
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
