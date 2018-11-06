package com.seancarroll.foundationdb.es;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static com.seancarroll.foundationdb.es.TestHelpers.byteArrayToString;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StreamMessageComparator {

    static boolean equal(NewStreamMessage expected, StreamMessage actual) {
        if (!Objects.equals(expected.getMessageId(), actual.getMessageId())) {
            return false;
        }

        if (!Objects.equals(expected.getType(), actual.getType())) {
            return false;
        }

        String expectedDataString = byteArrayToString(expected.getData());
        String expectedMetadataString = byteArrayToString(expected.getMetadata());

        String actualDataString = byteArrayToString(actual.getData());
        String actualMetadataDataString = byteArrayToString(actual.getMetadata());

        return Objects.equals(expectedDataString, actualDataString)
            && Objects.equals(expectedMetadataString, actualMetadataDataString);
    }

    static boolean equal(NewStreamMessage[] expected, StreamMessage[] actual) {
        if (expected.length != actual.length) {
            return false;
        }

        for (int i = 0; i < expected.length; i++) {
            if (!equal(expected[i], actual[i])) {
                return false;
            }
        }

        return true;
    }


}
