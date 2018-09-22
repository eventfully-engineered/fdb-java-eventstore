package com.seancarroll.foundationdb.es;

// TODO: We dont need StreamPosition and StreamVersion
/**
 * Constants for stream positions
 */
public class StreamPosition {

    /**
     * The first event in a stream
     */
    public static final int START = 1;

    /**
     *The last event in the stream
     */
    public static final int END = -1;

    private StreamPosition() {
        // public static fields only
    }

}
