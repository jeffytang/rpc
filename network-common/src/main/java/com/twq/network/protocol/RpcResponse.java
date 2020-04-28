package com.twq.network.protocol;

import com.google.common.base.Objects;
import com.twq.network.buffer.ManagedBuffer;
import com.twq.network.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;
/** Response to {@link RpcRequest} for a successful RPC. */
public final class RpcResponse extends AbstractResponseMessage {
  public final long requestId;

  public RpcResponse(long requestId, ManagedBuffer message) {
    super(message, true);
    this.requestId = requestId;
  }

  @Override
  public Type type() { return Type.RpcResponse; }

  @Override
  public int encodedLength() {
    // The integer (a.k.a. the body size) is not really used, since that information is already
    // encoded in the frame length. But this maintains backwards compatibility with versions of
    // RpcRequest that use Encoders.ByteArrays.
    return 8 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    // See comment in encodedLength().
    buf.writeInt((int) body().size());
  }

  @Override
  public ResponseMessage createFailureResponse(String error) {
    return new RpcFailure(requestId, error);
  }

  public static RpcResponse decode(ByteBuf buf) {
    long requestId = buf.readLong();
    // See comment in encodedLength().
    buf.readInt();
    return new RpcResponse(requestId, new NettyManagedBuffer(buf.retain()));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcResponse) {
      RpcResponse o = (RpcResponse) other;
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
