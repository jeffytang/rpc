package com.twq.network;

import com.google.common.io.Files;
import com.twq.network.buffer.FileSegmentManagedBuffer;
import com.twq.network.buffer.ManagedBuffer;
import com.twq.network.buffer.NioManagedBuffer;
import com.twq.network.config.TransportConf;
import com.twq.network.util.JavaUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class StreamTestHelper {
    // static final String[] STREAMS = { "largeBuffer", "smallBuffer", "emptyBuffer", "file" };
    static final String[] STREAMS = {"file"};

    final File testFile;
    final File tempDir;

    final ByteBuffer emptyBuffer;
    final ByteBuffer smallBuffer;
    final ByteBuffer largeBuffer;

    private static ByteBuffer createBuffer(int bufSize) {
        ByteBuffer buf = ByteBuffer.allocate(bufSize);
        for (int i = 0; i < bufSize; i ++) {
            buf.put((byte) i);
        }
        buf.flip();
        return buf;
    }

    StreamTestHelper() throws Exception {
        tempDir = Files.createTempDir();
        emptyBuffer = createBuffer(0);
        smallBuffer = createBuffer(100);
        largeBuffer = createBuffer(100000);

        testFile = File.createTempFile("stream-test-file", "txt", tempDir);
        FileOutputStream fp = new FileOutputStream(testFile);
        try {
            Random rnd = new Random();
            for (int i = 0; i < 512; i++) {
                byte[] fileContent = new byte[1024];
                rnd.nextBytes(fileContent);
                fp.write(fileContent);
            }
        } finally {
            fp.close();
        }
    }

    public ByteBuffer srcBuffer(String name) {
        switch (name) {
            case "largeBuffer":
                return largeBuffer;
            case "smallBuffer":
                return smallBuffer;
            case "emptyBuffer":
                return emptyBuffer;
            default:
                throw new IllegalArgumentException("Invalid stream: " + name);
        }
    }

    public ManagedBuffer openStream(TransportConf conf, String streamId) {
        switch (streamId) {
            case "file":
                return new FileSegmentManagedBuffer(conf, testFile, 0, testFile.length());
            default:
                return new NioManagedBuffer(srcBuffer(streamId));
        }
    }

    void cleanup() {
        if (tempDir != null) {
            try {
                JavaUtils.deleteRecursively(tempDir);
            } catch (IOException io) {
                throw new RuntimeException(io);
            }
        }
    }
}
