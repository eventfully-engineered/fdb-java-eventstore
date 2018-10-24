package com.seancarroll.foundationdb.es;

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

}
