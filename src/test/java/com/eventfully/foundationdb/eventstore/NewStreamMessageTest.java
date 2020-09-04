package com.eventfully.foundationdb.eventstore;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

class NewStreamMessageTest {

    @Test
    void shouldThrowWhenMessageSizeExceedsMaximum() {
        String data = StringUtils.repeat("a", 100_000);
        assertThrows(IllegalArgumentException.class, () -> new NewStreamMessage(UUID.randomUUID(), "SomeType", data.getBytes(), "metadata".getBytes()));
    }

}
