package com.eventfully.foundationdb.eventstore;

// TODO: We dont need StreamPosition and StreamVersion.
// StreamVersion vs StreamPosition???
/**
 *
 *
 */
public final class StreamVersion {

    /**
     * The first message in a stream
     */
    public static final int START = 0;

    /**
     * the last message in a stream
     */
    public static final int END = -1;


    /**
     * No stream version
     */
    public static final int NONE = -2; // TODO: does -2 work?

    private StreamVersion() {
        // public static constants only
    }
}
