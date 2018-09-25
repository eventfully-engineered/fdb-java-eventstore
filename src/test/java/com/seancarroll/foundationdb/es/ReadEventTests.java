package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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

            assertThrows(IllegalArgumentException.class, () -> es.readEvent(null, 0));
        }
    }

    @Test
    public void shouldThrowWhenStreamIdIsEmpty() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readEvent("", 0));
        }
    }

    @Test
    public void shouldThrowWhenEventNumberIsLessThanNegativeOne() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readEvent("test-stream", -1));
        }
    }

    @Test
    public void shouldNotifyUsingStatusCodeIfStreamNotFound() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            ReadEventResult read = es.readEvent("test-stream", 1);

            assertEquals(ReadEventStatus.NO_STREAM, read.getStatus());
            assertNull(read.getEvent());
            assertEquals("test-stream", read.getStream());
            assertEquals(1, read.getEventNumber());
        }
    }

    @Test
    public void shouldReturnNoStreamIfRequestedLastEventInEmptyStream() {

        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            ReadEventResult read = es.readEvent("test-stream", 1);

            assertEquals(ReadEventStatus.NO_STREAM, read.getStatus());
        }
    }

    @Test
    public void shouldNotifyUsingStatusCodeWhenStreamIsDeleted() {

    }

    @Test
    public void shouldNotifyUsingStatusCodeWhenStreamDoesNotHaveEvent() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadEventResult read = es.readEvent(stream, 10);

            assertEquals(ReadEventStatus.NOT_FOUND, read.getStatus());
            assertNull(read.getEvent());
            assertEquals("test-stream", read.getStream());
            assertEquals(10, read.getEventNumber());
        }
    }

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
