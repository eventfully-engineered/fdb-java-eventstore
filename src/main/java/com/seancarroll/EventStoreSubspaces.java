package com.seancarroll;

// rather than subspaces could we them prefix instead?
// not a huge fan of aggregate...what about stream?
// what about keyspaces? GLOBAL vs STREAM?
public enum EventStoreSubspaces {

    GLOBAL("0"),
    STREAM("1");

    private final String value;

    EventStoreSubspaces(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
