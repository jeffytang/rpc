package com.twq.network.protocol;

import com.twq.network.buffer.ManagedBuffer;

/**
 * Abstract class for response messages.
 */
public abstract class AbstractResponseMessage
        extends AbstractMessage
        implements ResponseMessage {

    protected AbstractResponseMessage(ManagedBuffer body, boolean isBodyInFrame) {
        super(body, isBodyInFrame);
    }

    public abstract ResponseMessage createFailureResponse(String error);

}
