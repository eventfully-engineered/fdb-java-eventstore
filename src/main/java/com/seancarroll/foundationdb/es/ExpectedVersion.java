package com.seancarroll.foundationdb.es;

/**
 * Expected version constants
 *
 */
public final class ExpectedVersion {

    /**
     * Stream does not exist
     */
    public static final int NO_STREAM = -1;


    /**
     * Any version
     */
    public static final int ANY = -2;

    /**
     * The stream should exist.
     * If it or a metadata stream does not exist treat that as a concurrency problem.
     */
    public static final int STREAM_EXISTS = -4;


    private ExpectedVersion() {
        // static constants only
    }

}

