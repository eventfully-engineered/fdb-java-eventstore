package com.seancarroll.foundationdb.es;

/**
 * Represents the status of a event read.
 */
public enum ReadEventStatus {

    /**
     * The read operation was successful.
     */
    SUCCESS,

    /**
     * The event was not found.
     */
    NOT_FOUND,

    /**
     * The stream previously existed but was deleted
     */
    STREAM_DELETED
}
