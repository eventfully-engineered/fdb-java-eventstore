package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ReadHeadPositionTests extends TestFixture {

    @BeforeEach
    void clean() {
        FDB fdb = FDB.selectAPIVersion(520);
        TestHelpers.clean(fdb);
    }

    @Test
    void shouldReturnNullWhenNoEvents() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            Versionstamp headPosition = es.readHeadPosition();

            assertNull(headPosition);
        }
    }

    @Test
    void shouldReturnHeadEvent() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.NO_STREAM, messages);

            Versionstamp headPosition = es.readHeadPosition();

            assertEquals(-1, Position.START.compareTo(headPosition));
        }
    }

}
