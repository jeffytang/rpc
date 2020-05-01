package com.twq.network;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.twq.network.buffer.ManagedBuffer;
import com.twq.network.buffer.NioManagedBuffer;
import com.twq.network.client.RpcResponseCallback;
import com.twq.network.client.StreamCallbackWithID;
import com.twq.network.client.TransportClient;
import com.twq.network.client.TransportClientFactory;
import com.twq.network.config.MapConfigProvider;
import com.twq.network.config.TransportConf;
import com.twq.network.server.RpcHandler;
import com.twq.network.server.StreamManager;
import com.twq.network.server.TransportServer;
import com.twq.network.util.JavaUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class RpcIntegrationSuite {
    static TransportConf conf;
    static TransportServer server;
    static TransportClientFactory clientFactory;
    static RpcHandler rpcHandler;
    static List<String> oneWayMsgs;
    static StreamTestHelper testData;

    static ConcurrentHashMap<String, VerifyingStreamCallback> streamCallbacks =
            new ConcurrentHashMap<>();

    @BeforeClass
    public static void setUp() throws Exception {
        conf = new TransportConf(MapConfigProvider.EMPTY);
        testData = new StreamTestHelper();
        rpcHandler = new RpcHandler() {
            @Override
            public void receive(TransportClient client,
                                ByteBuffer message,
                                RpcResponseCallback callback) {
                String msg = JavaUtils.bytesToString(message);
                String[] parts = msg.split("/");
                if (parts[0].equals("hello")) {
                    callback.onSuccess(JavaUtils.stringToBytes("Hello, " + parts[1] + "!"));
                } else if (parts[0].equals("return error")) {
                    callback.onFailure(new RuntimeException("Returned: " + parts[1]));
                } else if (parts[0].equals("throw error")) {
                    throw new RuntimeException("Thrown: " + parts[1]);
                }
            }

            @Override
            public StreamCallbackWithID receiveStream(TransportClient client, ByteBuffer messageHeader, RpcResponseCallback callback) {
                return receiveStreamHelper(JavaUtils.bytesToString(messageHeader));
            }

            @Override
            public void receive(TransportClient client, ByteBuffer message) {
                oneWayMsgs.add(JavaUtils.bytesToString(message));
            }

            @Override
            public StreamManager getStreamManager() {
                return new OneForOneStreamManager();
            }
        };
        TransportContext context = new TransportContext(conf, rpcHandler);
        server = context.createServer();
        clientFactory = context.createClientFactory();
        oneWayMsgs = new ArrayList<>();
    }

    private static StreamCallbackWithID receiveStreamHelper(String msg) {
        try {
            if (msg.startsWith("fail/")) {
                String[] parts = msg.split("/");
                switch (parts[1]) {
                    case "exception-ondata":
                        return new StreamCallbackWithID() {
                            @Override
                            public void onData(String streamId, ByteBuffer buf) throws IOException {
                                throw new IOException("failed to read stream data!");
                            }

                            @Override
                            public void onComplete(String streamId) throws IOException {
                            }

                            @Override
                            public void onFailure(String streamId, Throwable cause) throws IOException {
                            }

                            @Override
                            public String getID() {
                                return msg;
                            }
                        };
                    case "exception-oncomplete":
                        return new StreamCallbackWithID() {
                            @Override
                            public void onData(String streamId, ByteBuffer buf) throws IOException {
                            }

                            @Override
                            public void onComplete(String streamId) throws IOException {
                                throw new IOException("exception in onComplete");
                            }

                            @Override
                            public void onFailure(String streamId, Throwable cause) throws IOException {
                            }

                            @Override
                            public String getID() {
                                return msg;
                            }
                        };
                    case "null":
                        return null;
                    default:
                        throw new IllegalArgumentException("unexpected msg: " + msg);
                }
            } else {
                VerifyingStreamCallback streamCallback = new VerifyingStreamCallback(msg);
                streamCallbacks.put(msg, streamCallback);
                return streamCallback;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class VerifyingStreamCallback implements StreamCallbackWithID {
        final String streamId;
        final StreamTestCallback helper;
        final OutputStream out;
        final File outFile;

        VerifyingStreamCallback(String streamId) throws IOException {
            if (streamId.equals("file")) {
                outFile = File.createTempFile("data", ".tmp", testData.tempDir);
                out = new FileOutputStream(outFile);
            } else {
                out = new ByteArrayOutputStream();
                outFile = null;
            }
            this.streamId = streamId;
            helper = new StreamTestCallback(out);
        }

        void verify() throws IOException {
            if (streamId.equals("file")) {
                assertTrue("File stream did not match.", Files.equal(testData.testFile, outFile));
            } else {
                byte[] result = ((ByteArrayOutputStream)out).toByteArray();
                ByteBuffer srcBuffer = testData.srcBuffer(streamId);
                ByteBuffer base;
                synchronized (srcBuffer) {
                    base = srcBuffer.duplicate();
                }
                byte[] expected = new byte[base.remaining()];
                base.get(expected);
                assertEquals(expected.length, result.length);
                assertTrue("buffers don't match", Arrays.equals(expected, result));
            }
        }

        @Override
        public String getID() {
            return streamId;
        }

        @Override
        public void onData(String streamId, ByteBuffer buf) throws IOException {
            helper.onData(streamId, buf);
        }

        @Override
        public void onComplete(String streamId) throws IOException {
            helper.onComplete(streamId);
        }

        @Override
        public void onFailure(String streamId, Throwable cause) throws IOException {
            helper.onFailure(streamId, cause);
        }
    }

    @AfterClass
    public static void tearDown() {
        server.close();
        clientFactory.close();
        testData.cleanup();
    }

    static class RpcResult {
        public Set<String> successMessages;
        public Set<String> errorMessages;
    }

    private RpcResult sendRPC(String ... commands) throws Exception {
        TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
        final Semaphore sem = new Semaphore(0);

        final RpcResult res = new RpcResult();
        res.successMessages = Collections.synchronizedSet(new HashSet<String>());
        res.errorMessages = Collections.synchronizedSet(new HashSet<String>());

        RpcResponseCallback callback = new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer message) {
                String response = JavaUtils.bytesToString(message);
                res.successMessages.add(response);
                sem.release();
            }

            @Override
            public void onFailure(Throwable e) {
                res.errorMessages.add(e.getMessage());
                sem.release();
            }
        };

        for (String command : commands) {
            client.sendRpc(JavaUtils.stringToBytes(command), callback);
        }

        if (!sem.tryAcquire(commands.length, 5, TimeUnit.HOURS)) {
            fail("Timeout getting response from the server");
        }
        client.close();
        return res;
    }

    private RpcResult sendRpcWithStream(String... streams) throws Exception {
        TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
        final Semaphore sem = new Semaphore(0);
        RpcResult res = new RpcResult();
        res.successMessages = Collections.synchronizedSet(new HashSet<String>());
        res.errorMessages = Collections.synchronizedSet(new HashSet<String>());

        for (String stream : streams) {
            int idx = stream.lastIndexOf('/');
            ManagedBuffer meta = new NioManagedBuffer(JavaUtils.stringToBytes(stream));
            String streamName = (idx == -1) ? stream : stream.substring(idx + 1);
            ManagedBuffer data = testData.openStream(conf, streamName);
            client.uploadStream(meta, data, new RpcStreamCallback(stream, res, sem));
        }

        if (!sem.tryAcquire(streams.length, 5, TimeUnit.SECONDS)) {
            fail("Timeout getting response from the server");
        }
        streamCallbacks.values().forEach(streamCallback -> {
            try {
                streamCallback.verify();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        client.close();
        return res;
    }

    private static class RpcStreamCallback implements RpcResponseCallback {
        final String streamId;
        final RpcResult res;
        final Semaphore sem;

        RpcStreamCallback(String streamId, RpcResult res, Semaphore sem) {
            this.streamId = streamId;
            this.res = res;
            this.sem = sem;
        }

        @Override
        public void onSuccess(ByteBuffer message) {
            res.successMessages.add(streamId);
            sem.release();
        }

        @Override
        public void onFailure(Throwable e) {
            res.errorMessages.add(e.getMessage());
            sem.release();
        }
    }

    @Test
    public void singleRPC() throws Exception {
        RpcResult res = sendRPC("hello/Aaron");
        assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!"));
        assertTrue(res.errorMessages.isEmpty());
    }
}
