package com.eventfully.foundationdb.eventstore;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Versionstamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ReadHeadPositionTests extends TestFixture {

    private FDB fdb;

    @BeforeEach
    void clean() throws ExecutionException, InterruptedException {
        fdb = FDB.selectAPIVersion(610);
        TestHelpers.clean(fdb);
    }

    @Test
    void shouldReturnNullWhenNoEvents() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            Versionstamp headPosition = es.readHeadPosition().get();

            assertNull(headPosition);
        }
    }

    @Test
    void shouldReturnHeadEvent() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.NO_STREAM, messages).get();

            Versionstamp headPosition = es.readHeadPosition().get();

            assertEquals(-1, Position.START.compareTo(headPosition));
        }
    }

}
