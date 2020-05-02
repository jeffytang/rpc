package com.twq.network;

import com.google.common.io.Files;
import com.twq.network.buffer.ManagedBuffer;
import com.twq.network.client.RpcResponseCallback;
import com.twq.network.client.TransportClient;
import com.twq.network.client.TransportClientFactory;
import com.twq.network.config.MapConfigProvider;
import com.twq.network.config.TransportConf;
import com.twq.network.server.RpcHandler;
import com.twq.network.server.StreamManager;
import com.twq.network.server.TransportServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamSuite {
    private static final String[] STREAMS = StreamTestHelper.STREAMS;
    private static StreamTestHelper testData;

    private static TransportServer server;
    private static TransportClientFactory clientFactory;

    private static ByteBuffer createBuffer(int bufSize) {
        ByteBuffer buf = ByteBuffer.allocate(bufSize);
        for (int i = 0; i < bufSize; i ++) {
            buf.put((byte) i);
        }
        buf.flip();
        return buf;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        testData = new StreamTestHelper();

        final TransportConf conf = new TransportConf(MapConfigProvider.EMPTY);
        final StreamManager streamManager = new StreamManager() {
            @Override
            public ManagedBuffer getChunk(long streamId, int chunkIndex) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ManagedBuffer openStream(String streamId) {
                return testData.openStream(conf, streamId);
            }
        };
        RpcHandler handler = new RpcHandler() {
            @Override
            public void receive(
                    TransportClient client,
                    ByteBuffer message,
                    RpcResponseCallback callback) {
                throw new UnsupportedOperationException();
            }

            @Override
            public StreamManager getStreamManager() {
                return streamManager;
            }
        };
        TransportContext context = new TransportContext(conf, handler);
        server = context.createServer();
        clientFactory = context.createClientFactory();
    }

    @AfterClass
    public static void tearDown() {
        server.close();
        clientFactory.close();
        testData.cleanup();
    }

    @Test
    public void testSingleStream() throws Throwable {
        TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
        try {
            StreamTask task = new StreamTask(client, "smallBuffer", TimeUnit.HOURS.toMillis(5));
            task.run();
            task.check();
        } finally {
            client.close();
        }
    }

    private static class StreamTask implements Runnable {
        private final TransportClient client;
        private final String streamId;
        private final long timeoutMs;
        private Throwable error;

        StreamTask(TransportClient client, String streamId, long timeoutMs) {
            this.client = client;
            this.streamId = streamId;
            this.timeoutMs = timeoutMs;
        }

        @Override
        public void run() {
            ByteBuffer srcBuffer = null;
            OutputStream out = null;
            File outFile = null;

            try {
                ByteArrayOutputStream baos = null;

                switch (streamId) {
                    case "largeBuffer":
                        baos = new ByteArrayOutputStream();
                        out = baos;
                        srcBuffer = testData.largeBuffer;
                        break;
                    case "smallBuffer":
                        baos = new ByteArrayOutputStream();
                        out = baos;
                        srcBuffer = testData.smallBuffer;
                        break;
                    case "file":
                        outFile = File.createTempFile("data", ".tmp", testData.tempDir);
                        out = new FileOutputStream(outFile);
                        break;
                    case "emptyBuffer":
                        baos = new ByteArrayOutputStream();
                        out = baos;
                        srcBuffer = testData.emptyBuffer;
                        break;
                    default:
                        throw new IllegalArgumentException(streamId);
                }

                StreamTestCallback callback = new StreamTestCallback(out);
                client.stream(streamId, callback);
                callback.waitForCompletion(timeoutMs);

                if (srcBuffer == null) {
                    assertTrue("File stream did not match.", Files.equal(testData.testFile, outFile));
                } else {
                    ByteBuffer base;
                    synchronized (srcBuffer) {
                        base = srcBuffer.duplicate();
                    }
                    byte[] result = baos.toByteArray();
                    byte[] expected = new byte[base.remaining()];
                    base.get(expected);
                    assertEquals(expected.length, result.length);
                    assertTrue("buffers don't match", Arrays.equals(expected, result));
                }

            } catch (Throwable t) {
                error = t;
            } finally {
                if (out != null) {
                    try {
                        out.close();
                    } catch (Exception e) {
                        // ignore.
                    }
                }
                if (outFile != null) {
                    outFile.delete();
                }
            }
        }

        public void check() throws Throwable {
            if (error != null) {
                throw error;
            }
        }
    }
}
