package com.eventfully.foundationdb.eventstore;

/**
 * Exception thrown if the expected version specified on an operation does not match the version of the stream when
 * the operation was attempted.
 */
public class WrongExpectedVersionException extends RuntimeException {

    /**
     * Constructs new WrongExpectedVersionException
     * @param message Details about the exception
     */
    public WrongExpectedVersionException(String message) {
        super(message);
    }

    public WrongExpectedVersionException(String streamId, int version) {
        super(String.format("Append failed due to wrong expected version. Stream %s. Expected version: %d.", streamId, version));
    }

    public WrongExpectedVersionException(String streamId, long expectedVersion, long eventNumber) {
        super(String.format("Append failed due to wrong expected version. Stream %s. Expected version: %d. Current version %d.", streamId, expectedVersion, eventNumber));
    }

}
