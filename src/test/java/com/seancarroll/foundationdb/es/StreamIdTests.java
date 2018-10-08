package com.seancarroll.foundationdb.es;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StreamIdTests {

    @Test
    void shouldNotBeEqualToNull() {
        StreamId stream = new StreamId("");
        assertFalse(stream.equals(null));
    }

    @Test
    void shouldNotBeEqualToDifferentObject() {
        assertFalse(new StreamId("").equals(new Object()));
    }

    @Test
    void shouldNotBeEqualWhenOriginalStreamIdsAreDifferent() {
        assertFalse(new StreamId("id-1").equals(new StreamId("id-2")));
    }

    @Test
    void shouldBeEqualWhenOriginalStreamIdsAreSame() {
        assertTrue(new StreamId("id-1").equals(new StreamId("id-1")));
    }

    @Test
    void shouldBeEqualWhenSameStreamIdObject() {
        StreamId stream = new StreamId("");
        assertTrue(stream.equals(stream));
    }

}
