package com.seancarroll.foundationdb.es;

public enum EventStoreSubspaces {

    GLOBAL("0"),
    STREAM("1"),
    // TODO: do we need this?
    METADATA("2");

    private final String value;

    EventStoreSubspaces(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
