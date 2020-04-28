package com.twq.network.protocol;

import com.google.common.base.Objects;
import com.twq.network.buffer.ManagedBuffer;
import com.twq.network.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;

public class RpcRequest extends AbstractMessage implements RequestMessage {
    /** Used to link an RPC request with its response. */
    public final long requestId;

    public RpcRequest(long requestId, ManagedBuffer body) {
        super(body, true);
        this.requestId = requestId;
    }

    @Override
    public Type type() {
        return Type.RpcRequest;
    }

    @Override
    public int encodedLength() {
        // The integer (a.k.a. the body size) is not really used,
        // since that information is already encoded in the frame length.
        // But this maintains backwards compatibility with versions of
        // RpcRequest that use Encoders.ByteArrays.
        return 8 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(requestId);
        // See comment in encodedLength().
        buf.writeInt((int) body().size());
    }

    public static RpcRequest decode(ByteBuf buf) {
        long requestId = buf.readLong();
        // See comment in encodedLength().
        buf.readInt();
        return new RpcRequest(requestId, new NettyManagedBuffer(buf.retain()));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(requestId, body());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof RpcRequest) {
            RpcRequest o = (RpcRequest) other;
            return requestId == o.requestId && super.equals(o);
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("requestId", requestId)
                .add("body", body())
                .toString();
    }
}
