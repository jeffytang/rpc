package com.twq.network.buffer;

import com.google.common.base.Objects;
import com.google.common.io.ByteStreams;
import com.twq.network.config.TransportConf;
import com.twq.network.util.JavaUtils;
import com.twq.network.util.LimitedInputStream;
import io.netty.channel.DefaultFileRegion;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * A {@link ManagedBuffer} backed by a segment in a file.
 */
public class FileSegmentManagedBuffer implements ManagedBuffer {
    private final TransportConf conf;
    private final File file;
    private final long offset;
    private final long length;

    public FileSegmentManagedBuffer(TransportConf conf, File file, long offset, long length) {
        this.conf = conf;
        this.file = file;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        FileChannel channel = null;
        try {
            channel = new RandomAccessFile(file, "r").getChannel();
            if (length < conf.memoryMapBytes()) {
                ByteBuffer buf = ByteBuffer.allocate((int) length);
                channel.position(offset);
                while (buf.remaining() != 0) {
                    if (channel.read(buf) == -1) {
                        throw new IOException(String.format("Reached EOF before filling buffer\n" +
                                        "offset=%s\nfile=%s\nbuf.remaining=%s",
                                offset, file.getAbsoluteFile(), buf.remaining()));
                    }
                }
                buf.flip();
                return buf;
            } else {
                return channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
            }
        } catch (IOException e) {
            String errorMessage = "Error in reading " + this;
            try {
                if (channel != null) {
                    long size = channel.size();
                    errorMessage = "Error in reading " + this + " (actual file length " + size + ")";
                }
            } catch (IOException ignored) {
                // ignored
            }
            throw new IOException(errorMessage, e);
        } finally {
            JavaUtils.closeQuietly(channel);
        }

    }

    @Override
    public InputStream createInputStream() throws IOException {
        FileInputStream is = null;
        boolean shouldClose = true;
        try {
            is = new FileInputStream(file);
            ByteStreams.skipFully(is, offset);
            InputStream r = new LimitedInputStream(is, length);
            shouldClose = false;
            return r;
        } catch (IOException e) {
            String errorMessage = "Error in reading " + this;
            if (is != null) {
                long size = file.length();
                errorMessage = "Error in reading " + this + " (actual file length " + size + ")";
            }
            throw new IOException(errorMessage, e);
        } finally {
            if (shouldClose)
                JavaUtils.closeQuietly(is);
        }
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
        if (conf.lazyFileDescriptor()) {
            return new DefaultFileRegion(file, offset, length);
        } else {
            FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
            return new DefaultFileRegion(channel, offset, length);
        }
    }

    public File getFile() { return file; }

    public long getOffset() { return offset; }

    public long getLength() { return length; }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("file", file)
                .add("offset", offset)
                .add("length", length)
                .toString();
    }
}
