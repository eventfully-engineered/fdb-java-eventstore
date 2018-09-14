package com.seancarroll.foundationdb.es;

public class WrongExpectedVersionException extends RuntimeException {

    public WrongExpectedVersionException() {
        super();
    }

    public WrongExpectedVersionException(String message) {
        super(message);
    }

}
