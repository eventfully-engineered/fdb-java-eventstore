package com.seancarroll.foundationdb.es;

/**
 * Possible outcomes reading a slice of a stream
 *
 */
public enum SliceReadStatus {

    /**
     * Read was successful
     */
    SUCCESS,
    /**
     * Stream not found
     */
    STREAM_NOT_FOUND
}

