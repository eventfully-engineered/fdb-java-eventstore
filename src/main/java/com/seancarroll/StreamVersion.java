package com.seancarroll;

/**
 *
 *
 */
public class StreamVersion {

    /**
     * No stream version
     */
    public static final Integer NONE = null;

    /**
     * The first message in a stream
     */
    public static final int START = 0;

    /**
     * the last message in a stream
     */
    public static final int END = -1;

    private StreamVersion() {
        // static constants only
    }
}
