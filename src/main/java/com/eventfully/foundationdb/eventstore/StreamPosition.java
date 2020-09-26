package com.eventfully.foundationdb.eventstore;

// TODO: We dont need StreamPosition and StreamVersion
/**
 * Constants for stream positions
 */
public final class StreamPosition {

    /**
     * The first event in a stream
     */
    public static final long START = 0;

    /**
     *The last event in the stream
     */
    public static final long END = -1;

    private StreamPosition() {
        // public static fields only
    }

}
