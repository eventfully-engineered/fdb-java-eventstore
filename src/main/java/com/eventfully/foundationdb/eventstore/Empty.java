package com.eventfully.foundationdb.eventstore;

/**
 *
 */
public final class Empty {

    public static final byte[] BYTE_ARRAY = new byte[0];
    public static final StreamMessage[] STREAM_MESSAGES = new StreamMessage[0];

    private Empty() {
        // statics only
    }
}
