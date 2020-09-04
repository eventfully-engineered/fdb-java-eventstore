package com.eventfully.foundationdb.eventstore;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamIdTests {

    @Test
    void shouldNotBeEqualToNull() {
        StreamId stream = new StreamId("");
        assertNotEquals(stream, null);
    }

    @Test
    void shouldNotBeEqualToDifferentObject() {
        assertNotEquals(new Object(), new StreamId(""));
    }

    @Test
    void shouldNotBeEqualWhenOriginalStreamIdsAreDifferent() {
        assertNotEquals(new StreamId("id-2"), new StreamId("id-1"));
    }

    @Test
    void shouldBeEqualWhenOriginalStreamIdsAreSame() {
        assertEquals(new StreamId("id-1"), new StreamId("id-1"));
    }

    @Test
    void shouldBeEqualWhenSameStreamIdObject() {
        StreamId stream = new StreamId("");
        assertEquals(stream, stream);
    }

}
