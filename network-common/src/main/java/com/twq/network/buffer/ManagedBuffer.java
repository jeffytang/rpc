package com.twq.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 *
 */
public interface ManagedBuffer {

    // 当前 buffer 数据的字节数
    long size();

    // 将当前的 buffer 转成 Java Nio 的 ByteBuffer
    ByteBuffer nioByteBuffer() throws IOException;

    // 将当前的 buffer 转成 InputStream
    InputStream createInputStream() throws IOException;

    // 增加当前 buffer 的引用数
    ManagedBuffer retain();

    // 减少当前 buffer 的引用数，以及释放当前的 buffer
    ManagedBuffer release();

    // 将当前的 buffer 转成 Netty 的对象，用于写到网络中
    // 这个对象可以是：io.netty.buffer.ByteBuf 或者 io.netty.channel.FileRegion
    Object convertToNetty() throws IOException;
}
