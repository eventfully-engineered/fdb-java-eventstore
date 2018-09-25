package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ReadAllEventsBackwardTests extends TestFixture {

    @BeforeEach
    public void clean() {
        FDB fdb = FDB.selectAPIVersion(520);
        TestHelpers.clean(fdb);
    }

    @Test
    public void readAllBackwardsTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            // TODO: using Position.START here feels strange. EventStore uses Position.END which I think is more logical
            // However, based on documentation range should always be start, end, reverse where start and end are the same regardless of reverse
            ReadAllPage backwardPage = es.readAllBackwards(Position.START, 1);

            assertNotNull(backwardPage);
            assertEquals(1, backwardPage.getMessages().length);
            assertFalse(backwardPage.isEnd());
            assertTrue(backwardPage.getMessages()[0].getMessageId().toString().contains("5"));
            assertEquals("type", backwardPage.getMessages()[0].getType());
            assertTrue(new String(backwardPage.getMessages()[0].getMetadata()).contains("metadata"));
        }
    }

    @Test
    public void readAllBackwardsMultipleStreamTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);
            es.appendToStream("test-stream2", ExpectedVersion.ANY, messages);

            ReadAllPage forwardPage = es.readAllBackwards(Position.START, 4);

            assertNotNull(forwardPage);
            assertEquals(4, forwardPage.getMessages().length);
            assertTrue(forwardPage.isEnd());
            assertEquals("type", forwardPage.getMessages()[0].getType());
            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));

        }
    }

    @Test
    public void readAllBackwardNextPage() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            // TODO: improve test
            // Does start make sense here? What does EventStore do?
            ReadAllPage backwardsPage = es.readAllBackwards(Position.START, 1);
            assertNotNull(backwardsPage);
            assertTrue(backwardsPage.getMessages()[0].getMessageId().toString().contains("5"));

            ReadAllPage nextPage = backwardsPage.readNext();
            assertNotNull(nextPage);
            assertTrue(nextPage.getMessages()[0].getMessageId().toString().contains("4"));
        }
    }

    // return_empty_slice_if_asked_to_read_from_start
    // return_partial_slice_if_not_enough_events
    // return_events_in_reversed_order_compared_to_written
    // be_able_to_read_all_one_by_one_until_end_of_stream
    // be_able_to_read_events_slice_at_time
    // throw_when_got_int_max_value_as_maxcount

}
