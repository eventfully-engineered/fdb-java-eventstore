package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestHelpers {

    static void clean(FDB fdb) {
        try (Database db = fdb.open()) {
            db.run((Transaction tr) -> {
                DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
                tr.clear(eventStoreSubspace.range());
                return null;
            });
        }
    }

    private static DirectorySubspace createEventStoreSubspace(Database db) {
        return db.run((Transaction tr) -> {
            try {
                return new DirectoryLayer(true).createOrOpen(tr, Collections.singletonList("es")).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    static void assertEventDataEqual(NewStreamMessage expected, StreamMessage actual) {
        assertEquals(expected.getMessageId(), actual.getMessageId());
        assertEquals(expected.getType(), actual.getType());

        String expectedDataString = byteArrayToString(expected.getData());
        String expectedMetadataString = byteArrayToString(expected.getMetadata());

        String actualDataString = byteArrayToString(actual.getData());
        String actualMetadataDataString = byteArrayToString(actual.getMetadata());

        assertEquals(expectedDataString, actualDataString);
        assertEquals(expectedMetadataString, actualMetadataDataString);
    }

    static void assertEventDataEqual(NewStreamMessage[] expected, StreamMessage[] actual) {
        assertEquals(expected.length, actual.length);

        for (int i = 0; i < expected.length; i++) {
            assertEventDataEqual(expected[i], actual[i]);
        }
    }

    static String byteArrayToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
