package com.seancarroll.foundationdb.es;

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
