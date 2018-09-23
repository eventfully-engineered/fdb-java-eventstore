package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReadAllEventsForwardTests extends TestFixture {

    @BeforeEach
    public void clean() {
        FDB fdb = FDB.selectAPIVersion(520);
        TestHelpers.clean(fdb);
    }


    @Test
    public void readAllForwardTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadAllPage forwardPage = es.readAllForwards(Position.START, 1);
            assertNotNull(forwardPage);
            assertEquals(1, forwardPage.getMessages().length);
            assertFalse(forwardPage.isEnd());
            assertEquals("type", forwardPage.getMessages()[0].getType());
            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));
        }
    }

    @Test
    public void readAllForwardMultipleStreamTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);
            es.appendToStream("test-stream2", ExpectedVersion.ANY, messages);

            ReadAllPage forwardPage = es.readAllForwards(Position.START, 4);
            assertNotNull(forwardPage);
            assertEquals(4, forwardPage.getMessages().length);
            assertTrue(forwardPage.isEnd());
            assertEquals("type", forwardPage.getMessages()[0].getType());
            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));
        }
    }

    @Test
    public void readAllForwardNextPage() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            // TODO: improve test
            ReadAllPage forwardPage = es.readAllForwards(Position.START, 1);
            assertNotNull(forwardPage);
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));

            ReadAllPage nextPage = forwardPage.readNext();
            assertNotNull(nextPage);
            assertTrue(nextPage.getMessages()[0].getMessageId().toString().contains("2"));
        }
    }

    // return_empty_slice_if_asked_to_read_from_end
    // return_events_in_same_order_as_written
    // be_able_to_read_all_one_by_one_until_end_of_stream
    // be_able_to_read_events_slice_at_time
    // return_partial_slice_if_not_enough_events
    // throw_when_got_int_max_value_as_maxcount

}
