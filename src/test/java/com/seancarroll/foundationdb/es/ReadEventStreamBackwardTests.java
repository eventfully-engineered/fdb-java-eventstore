package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReadEventStreamBackwardTests extends TestFixture {

    @BeforeEach
    public void clean() {
        FDB fdb = FDB.selectAPIVersion(520);
        TestHelpers.clean(fdb);
    }

    @Test
    public void readStreamBackwards() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadStreamPage backwardPage = es.readStreamBackwards("test-stream", 0, 1);

            assertNotNull(backwardPage);
            assertEquals(1, backwardPage.getMessages().length);
            assertFalse(backwardPage.isEnd());
            assertTrue(backwardPage.getMessages()[0].getMessageId().toString().contains("5"));
            assertEquals("type", backwardPage.getMessages()[0].getType());
            assertTrue(new String(backwardPage.getMessages()[0].getMetadata()).contains("metadata"));
            assertEquals(4, backwardPage.getMessages()[0].getStreamVersion());
            assertEquals(3, backwardPage.getNextStreamVersion());
        }
    }

    @Test
    public void throwWhenReadBackwardsMaxCountExceedsMaxReadCount() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamBackwards("test-stream", 0, EventStoreLayer.MAX_READ_SIZE + 1));
        }
    }

    @Test
    public void shouldThrowIfCountLessThanOrEqualZero() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamBackwards("test-stream", 0, 0));
        }
    }

    @Test
    public void readStreamBackwardsNextPage() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadStreamPage backwardsPage = es.readStreamBackwards("test-stream", 0, 1);
            assertNotNull(backwardsPage);
            assertTrue(backwardsPage.getMessages()[0].getMessageId().toString().contains("5"));


            // TODO: improve test
            ReadStreamPage nextPage = backwardsPage.getNext();
            assertNotNull(nextPage);
            assertEquals(1, nextPage.getMessages().length);
            assertFalse(nextPage.isEnd());
            assertTrue(nextPage.getMessages()[0].getMessageId().toString().contains("4"));

        }
    }

    // throw_if_count_le_zero
    // notify_using_status_code_if_stream_not_found
    // notify_using_status_code_if_stream_was_deleted
    // return_no_events_when_called_on_empty_stream
    // return_partial_slice_if_no_enough_events_in_stream
    // return_events_reversed_compared_to_written
    // be_able_to_read_single_event_from_arbitrary_position
    // be_able_to_read_first_event
    // be_able_to_read_last_event
    // be_able_to_read_slice_from_arbitrary_position
    // throw_when_got_int_max_value_as_maxcount


}
