package com.seancarroll.foundationdb.es;

/**
 *
 */
public class EventStoreDirectoryException extends RuntimeException {

    /**
     *
     * @param message
     */
    public EventStoreDirectoryException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
