package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReadEventTests extends TestFixture {

    @BeforeEach
    public void clean() {
        FDB fdb = FDB.selectAPIVersion(520);
        TestHelpers.clean(fdb);
    }

    @Test
    public void shouldThrowWhenStreamIdIsNull() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(NullPointerException.class, () -> es.readEvent(null, 0));
        }
    }

    @Test
    public void shouldThrowWhenStreamIdIsEmpty() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(NullPointerException.class, () -> es.readEvent("", 0));
        }
    }

    // throw_if_event_number_is_less_than_minus_one
    @Test
    public void shouldThrowWhenEventNumberIsLessThanNegativeOne() {

    }

    // notify_using_status_code_if_stream_not_found
    @Test
    public void shouldNotifyUsingStatusCodeIfStreamNotFound() {

    }

    // return_no_stream_if_requested_last_event_in_empty_stream
    @Test
    public void shouldReturnNoStreamIfRequestedLastEventInEmptyStream() {

    }

    // notify_using_status_code_if_stream_was_deleted
    @Test
    public void shouldNotifyUsingStatusCodeWhenStreamIsDeleted() {

    }

    // notify_using_status_code_if_stream_does_not_have_event
    @Test
    public void shouldNotifyUsingStatusCodeWhenStreamDoesNotHaveEvent() {

    }

    // return_existing_event
    @Test
    public void shouldReturnExistingEvent() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadEventResult read = es.readEvent(stream, 2);

            assertEquals(ReadEventStatus.SUCCESS, read.getStatus());
            // Assert.AreEqual(res.Event.Value.OriginalEvent.EventId, _eventId0);
            assertEquals("test-stream", read.getStream());
            assertEquals(2, read.getEventNumber());
            // Assert.AreNotEqual(DateTime.MinValue, res.Event.Value.OriginalEvent.Created);
            //Assert.AreNotEqual(0, res.Event.Value.OriginalEvent.CreatedEpoch);

        }
    }

    // retrieve_the_is_json_flag_properly

    // return_last_event_in_stream_if_event_number_is_minus_one
    @Test
    public void shouldReturnLastEventInStreamIfEventNumberIsNegativeOne() {

    }

}
