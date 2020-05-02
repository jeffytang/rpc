package com.twq.network;

import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.twq.network.buffer.FileSegmentManagedBuffer;
import com.twq.network.buffer.ManagedBuffer;
import com.twq.network.buffer.NioManagedBuffer;
import com.twq.network.client.ChunkReceivedCallback;
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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ChunkFetchIntegrationSuite {
    static final long STREAM_ID = 1;
    static final int BUFFER_CHUNK_INDEX = 0;
    static final int FILE_CHUNK_INDEX = 1;

    static TransportServer server;
    static TransportClientFactory clientFactory;
    static StreamManager streamManager;
    static File testFile;

    static ManagedBuffer bufferChunk;
    static ManagedBuffer fileChunk;

    @BeforeClass
    public static void setUp() throws Exception {
        int bufSize = 100000;
        final ByteBuffer buf = ByteBuffer.allocate(bufSize);
        for (int i = 0; i < bufSize; i ++) {
            buf.put((byte) i);
        }
        buf.flip();
        bufferChunk = new NioManagedBuffer(buf);

        testFile = File.createTempFile("shuffle-test-file", "txt");
        testFile.deleteOnExit();
        RandomAccessFile fp = new RandomAccessFile(testFile, "rw");
        boolean shouldSuppressIOException = true;
        try {
            byte[] fileContent = new byte[1024];
            new Random().nextBytes(fileContent);
            fp.write(fileContent);
            shouldSuppressIOException = false;
        } finally {
            Closeables.close(fp, shouldSuppressIOException);
        }

        final TransportConf conf = new TransportConf(MapConfigProvider.EMPTY);
        fileChunk = new FileSegmentManagedBuffer(conf, testFile, 10, testFile.length() - 25);

        streamManager = new StreamManager() {
            @Override
            public ManagedBuffer getChunk(long streamId, int chunkIndex) {
                assertEquals(STREAM_ID, streamId);
                if (chunkIndex == BUFFER_CHUNK_INDEX) {
                    return new NioManagedBuffer(buf);
                } else if (chunkIndex == FILE_CHUNK_INDEX) {
                    return new FileSegmentManagedBuffer(conf, testFile, 10, testFile.length() - 25);
                } else {
                    throw new IllegalArgumentException("Invalid chunk index: " + chunkIndex);
                }
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
        bufferChunk.release();
        server.close();
        clientFactory.close();
        testFile.delete();
    }

    static class FetchResult {
        public Set<Integer> successChunks;
        public Set<Integer> failedChunks;
        public List<ManagedBuffer> buffers;

        public void releaseBuffers() {
            for (ManagedBuffer buffer : buffers) {
                buffer.release();
            }
        }
    }

    private FetchResult fetchChunks(List<Integer> chunkIndices) throws Exception {
        TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
        final Semaphore sem = new Semaphore(0);

        final FetchResult res = new FetchResult();
        res.successChunks = Collections.synchronizedSet(new HashSet<Integer>());
        res.failedChunks = Collections.synchronizedSet(new HashSet<Integer>());
        res.buffers = Collections.synchronizedList(new LinkedList<ManagedBuffer>());

        ChunkReceivedCallback callback = new ChunkReceivedCallback() {
            @Override
            public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
                buffer.retain();
                res.successChunks.add(chunkIndex);
                res.buffers.add(buffer);
                sem.release();
            }

            @Override
            public void onFailure(int chunkIndex, Throwable e) {
                res.failedChunks.add(chunkIndex);
                sem.release();
            }
        };

        for (int chunkIndex : chunkIndices) {
            client.fetchChunk(STREAM_ID, chunkIndex, callback);
        }
        if (!sem.tryAcquire(chunkIndices.size(), 5, TimeUnit.HOURS)) {
            fail("Timeout getting response from the server");
        }
        client.close();
        return res;
    }

    @Test
    public void fetchBufferChunk() throws Exception {
        FetchResult res = fetchChunks(Arrays.asList(BUFFER_CHUNK_INDEX));
        assertEquals(Sets.newHashSet(BUFFER_CHUNK_INDEX), res.successChunks);
        assertTrue(res.failedChunks.isEmpty());
        assertBufferListsEqual(Arrays.asList(bufferChunk), res.buffers);
        res.releaseBuffers();
    }

    @Test
    public void fetchFileChunk() throws Exception {
        FetchResult res = fetchChunks(Arrays.asList(FILE_CHUNK_INDEX));
        assertEquals(Sets.newHashSet(FILE_CHUNK_INDEX), res.successChunks);
        assertTrue(res.failedChunks.isEmpty());
        assertBufferListsEqual(Arrays.asList(fileChunk), res.buffers);
        res.releaseBuffers();
    }

    private static void assertBufferListsEqual(List<ManagedBuffer> list0, List<ManagedBuffer> list1)
            throws Exception {
        assertEquals(list0.size(), list1.size());
        for (int i = 0; i < list0.size(); i ++) {
            assertBuffersEqual(list0.get(i), list1.get(i));
        }
    }

    private static void assertBuffersEqual(ManagedBuffer buffer0, ManagedBuffer buffer1)
            throws Exception {
        ByteBuffer nio0 = buffer0.nioByteBuffer();
        ByteBuffer nio1 = buffer1.nioByteBuffer();

        int len = nio0.remaining();
        assertEquals(nio0.remaining(), nio1.remaining());
        for (int i = 0; i < len; i ++) {
            assertEquals(nio0.get(), nio1.get());
        }
    }
}
