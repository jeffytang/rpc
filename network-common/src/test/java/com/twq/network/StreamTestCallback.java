package com.twq.network;

import com.twq.network.client.StreamCallback;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StreamTestCallback implements StreamCallback {
    private final OutputStream out;
    public volatile boolean completed;
    public volatile Throwable error;

    StreamTestCallback(OutputStream out) {
        this.out = out;
        this.completed = false;
    }

    @Override
    public void onData(String streamId, ByteBuffer buf) throws IOException {
        byte[] tmp = new byte[buf.remaining()];
        buf.get(tmp);
        out.write(tmp);
    }

    @Override
    public void onComplete(String streamId) throws IOException {
        out.close();
        synchronized (this) {
            completed = true;
            notifyAll();
        }
    }

    @Override
    public void onFailure(String streamId, Throwable cause) throws IOException {
        error = cause;
        synchronized (this) {
            completed = true;
            notifyAll();
        }
    }

    void waitForCompletion(long timeoutMs) {
        long now = System.currentTimeMillis();
        long deadline = now + timeoutMs;
        synchronized (this) {
            while (!completed && now < deadline) {
                try {
                    wait(deadline - now);
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
                now = System.currentTimeMillis();
            }
        }
        assertTrue("Timed out waiting for stream.", completed);
        assertNull(error);
    }
}
