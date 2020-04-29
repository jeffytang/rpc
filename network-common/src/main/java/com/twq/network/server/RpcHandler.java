package com.twq.network.server;

import com.twq.network.client.RpcResponseCallback;
import com.twq.network.client.TransportClient;

import java.nio.ByteBuffer;

/**
 * Handler for sendRPC() messages sent by {@link com.twq.network.client.TransportClient}s.
 */
public abstract class RpcHandler {
    /**
     * Receive a single RPC message. Any exception thrown while in this method will be sent back to
     * the client in string form as a standard RPC failure.
     *
     * Neither this method nor #receiveStream will be called in parallel for a single
     * TransportClient (i.e., channel).
     *
     * @param client A channel client which enables the handler to make requests back to the sender
     *               of this RPC. This will always be the exact same object for a particular channel.
     * @param message The serialized bytes of the RPC.
     * @param callback Callback which should be invoked exactly once upon success or failure of the
     *                 RPC.
     */
    public abstract void receive(
            TransportClient client,
            ByteBuffer message,
            RpcResponseCallback callback);

    /**
     * Invoked when the channel associated with the given client is active.
     */
    public void channelActive(TransportClient client) { }

    /**
     * Invoked when the channel associated with the given client is inactive.
     * No further requests will come from this client.
     */
    public void channelInactive(TransportClient client) { }

    public void exceptionCaught(Throwable cause, TransportClient client) { }
}
