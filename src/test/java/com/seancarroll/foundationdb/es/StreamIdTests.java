package com.seancarroll.foundationdb.es;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class StreamIdTests {

    @Test
    public void shouldNotBeEqualToNull() {
        StreamId stream = new StreamId("");
        assertFalse(stream.equals(null));
    }

    @Test
    public void shouldNotBeEqualToDifferentObject() {
        assertFalse(new StreamId("").equals(new Object()));
    }

    @Test
    public void shouldNotBeEqualWhenOriginalStreamIdsAreDifferent() {
        assertFalse(new StreamId("id-1").equals(new StreamId("id-2")));
    }

    @Test
    public void shouldBeEqualWhenOriginalStreamIdsAreSame() {
        assertTrue(new StreamId("id-1").equals(new StreamId("id-1")));
    }

    @Test
    public void shouldBeEqualWhenSameStreamIdObject() {
        StreamId stream = new StreamId("");
        assertTrue(stream.equals(stream));
    }

}
