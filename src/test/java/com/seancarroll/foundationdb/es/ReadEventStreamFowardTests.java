package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ReadEventStreamFowardTests extends TestFixture {

    @BeforeEach
    public void clean() {
        FDB fdb = FDB.selectAPIVersion(520);
        TestHelpers.clean(fdb);
    }

    @Test
    public void streamNotFoundTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            ReadStreamPage forwardPage = es.readStreamForwards("test-stream", 0, 1);

            assertNotNull(forwardPage);
            assertEquals(0, forwardPage.getMessages().length);
            assertTrue(forwardPage.isEnd());
            assertEquals(PageReadStatus.STREAM_NOT_FOUND, forwardPage.getStatus());
            // TODO: ugh dont like the need to call intValue...Fix
            assertEquals(StreamVersion.END, forwardPage.getNextStreamVersion());
        }
    }

    @Test
    public void readStreamForward() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadStreamPage forwardPage = es.readStreamForwards("test-stream", 0, 1);

            assertNotNull(forwardPage);
            assertEquals(1, forwardPage.getMessages().length);
            assertFalse(forwardPage.isEnd());
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));
            assertEquals("type", forwardPage.getMessages()[0].getType());
            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));
            assertEquals(0, forwardPage.getMessages()[0].getStreamVersion());
            assertEquals(1, forwardPage.getNextStreamVersion());
        }
    }

    @Test
    public void throwWhenMaxCountExceedsMaxReadCount() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", 0, EventStoreLayer.MAX_READ_SIZE + 1));

        }
    }

    @Test
    public void shouldThrowIfCountLessThanOrEqualZero() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", 0, 0));
        }
    }

    @Test
    public void readStreamForwardNextPage() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadStreamPage forwardPage = es.readStreamForwards("test-stream", 0, 1);
            assertNotNull(forwardPage);
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));


            // TODO: improve test
            ReadStreamPage nextPage = forwardPage.getNext();
            assertNotNull(nextPage);
            assertEquals(1, nextPage.getMessages().length);
            assertFalse(nextPage.isEnd());
            assertTrue(nextPage.getMessages()[0].getMessageId().toString().contains("2"));

        }
    }


    // throw_if_count_le_zero
    // throw_if_start_lt_zero
    // notify_using_status_code_if_stream_not_found
    // notify_using_status_code_if_stream_was_deleted
    // return_no_events_when_called_on_empty_stream
    // return_empty_slice_when_called_on_non_existing_range
    // return_partial_slice_if_not_enough_events_in_stream
    // throw_when_got_int_max_value_as_maxcount
    // return_events_in_same_order_as_written
    // be_able_to_read_single_event_from_arbitrary_position
    // be_able_to_read_slice_from_arbitrary_position
}
