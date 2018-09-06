package com.seancarroll;

/**
 * From SqlStreamStore
 * EventStore has SliceReadStatus
 * Represents the status of a page read.
 */
public enum PageReadStatus {

    SUCCESS,
    STREAM_NOT_FOUND
}

