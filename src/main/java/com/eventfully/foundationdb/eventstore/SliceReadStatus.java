package com.eventfully.foundationdb.eventstore;

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

