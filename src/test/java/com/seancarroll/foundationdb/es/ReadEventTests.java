package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class ReadEventTests extends TestFixture {

    private FDB fdb;

    @BeforeEach
    void clean() throws ExecutionException, InterruptedException {
        fdb = FDB.selectAPIVersion(600);
        TestHelpers.clean(fdb);
    }

    @Test
    void shouldThrowWhenStreamIdIsNull() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            assertThrows(IllegalArgumentException.class, () -> es.readEvent(null, 0));
        }
    }

    @Test
    void shouldThrowWhenStreamIdIsEmpty() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            assertThrows(IllegalArgumentException.class, () -> es.readEvent("", 0));
        }
    }

    @Test
    void shouldThrowWhenEventNumberIsLessThanNegativeOne() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            assertThrows(IllegalArgumentException.class, () -> es.readEvent("test-stream", -2));
        }
    }

    // TODO: check tests especially the next two below
    @Test
    void shouldNotifyUsingStatusCodeIfStreamNotFound() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            ReadEventResult read = es.readEvent("test-stream", 1).get();

            assertEquals(ReadEventStatus.NOT_FOUND, read.getStatus());
            assertNull(read.getEvent());
            assertEquals("test-stream", read.getStream());
            assertEquals(1, read.getEventNumber());
        }
    }

    @Test
    void shouldReturnNoStreamIfRequestedLastEventInEmptyStream() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            ReadEventResult read = es.readEvent("test-stream", 1).get();

            assertEquals(ReadEventStatus.NOT_FOUND, read.getStatus());
        }
    }

//    @Test
//    void shouldNotifyUsingStatusCodeWhenStreamIsDeleted() {
//        fail("not implemented");
//    }

    @Test
    void shouldNotifyUsingStatusCodeWhenStreamDoesNotHaveEvent() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadEventResult read = es.readEvent(stream, 10).get();

            assertEquals(ReadEventStatus.NOT_FOUND, read.getStatus());
            assertNull(read.getEvent());
            assertEquals("test-stream", read.getStream());
            assertEquals(10, read.getEventNumber());
        }
    }

    @Test
    void shouldReturnExistingEvent() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadEventResult read = es.readEvent(stream, 2).get();

            assertEquals(ReadEventStatus.SUCCESS, read.getStatus());
            // Assert.AreEqual(res.Event.Value.OriginalEvent.EventId, _eventId0);
            assertEquals("test-stream", read.getStream());
            assertEquals(2, read.getEventNumber());
            TestHelpers.assertEventDataEqual(messages[2], read.getEvent());
            // TODO: assert time is not null
            // TODO: assert position not null...maybe greater than Position.Start and less than Position.END
            // assert version not null
            // Assert.AreNotEqual(DateTime.MinValue, res.Event.Value.OriginalEvent.Created);
            //Assert.AreNotEqual(0, res.Event.Value.OriginalEvent.CreatedEpoch);

        }
    }

    // retrieve_the_is_json_flag_properly

    @Test
    void shouldReturnLastEventInStreamIfEventNumberIsNegativeOne() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadEventResult read = es.readEvent(stream, -1).get();

            TestHelpers.assertEventDataEqual(messages[4], read.getEvent());
        }
    }

    // TODO: add test where we append to one stream try and get end of another stream and verify we get a not found
    // shouldnt read from the other stream
}
