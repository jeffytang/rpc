package com.twq.network.buffer;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 *  A {@link ManagedBuffer} backed by {@link ByteBuffer}.
 */
public class NioManagedBuffer implements ManagedBuffer {
    private final ByteBuffer buffer;

    public NioManagedBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public long size() {
        return buffer.remaining();
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        return buffer.duplicate();
    }

    @Override
    public InputStream createInputStream() throws IOException {
        return new ByteBufInputStream(Unpooled.wrappedBuffer(buffer));
    }

    @Override
    public ManagedBuffer retain() {
        return this;
    }

    @Override
    public ManagedBuffer release() {
        return this;
    }

    @Override
    public Object convertToNetty() throws IOException {
        return Unpooled.wrappedBuffer(buffer);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("buf", buffer)
                .toString();
    }
}
