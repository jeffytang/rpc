package com.twq.network.client;

/**
 * General exception caused by a remote exception while fetching a chunk.
 */
public class ChunkFetchFailureException extends RuntimeException {
    public ChunkFetchFailureException(String errorMsg, Throwable cause) {
        super(errorMsg, cause);
    }

    public ChunkFetchFailureException(String errorMsg) {
        super(errorMsg);
    }
}
