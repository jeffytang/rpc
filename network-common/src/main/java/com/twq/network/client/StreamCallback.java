package com.twq.network.client;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Callback for streaming data. Stream data will be offered to the
 * {@link #onData(String, ByteBuffer)} method as it arrives. Once all the stream data is received,
 * {@link #onComplete(String)} will be called.
 * <p>
 * The network library guarantees that a single thread will call these methods at a time, but
 * different call may be made by different threads.
 */
public interface StreamCallback {
  /** Called upon receipt of stream data. */
  void onData(String streamId, ByteBuffer buf) throws IOException;

  /** Called when all data from the stream has been received. */
  void onComplete(String streamId) throws IOException;

  /** Called if there's an error reading data from the stream. */
  void onFailure(String streamId, Throwable cause) throws IOException;
}
