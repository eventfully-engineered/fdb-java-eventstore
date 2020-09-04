package com.eventfully.foundationdb.eventstore;

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
