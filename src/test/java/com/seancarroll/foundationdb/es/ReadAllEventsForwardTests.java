package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.collect.ObjectArrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.seancarroll.foundationdb.es.TestHelpers.assertEventDataEqual;
import static org.junit.jupiter.api.Assertions.*;

public class ReadAllEventsForwardTests extends TestFixture {

    @BeforeEach
    public void clean() {
        FDB fdb = FDB.selectAPIVersion(520);
        TestHelpers.clean(fdb);
    }

    // return_empty_slice_if_asked_to_read_from_end --> not sure this makes sense.
    // If I read from the start I would get that starting event...so why would it be different if I read from the end?
    // I should get the end event...no?
    // what about backward with the START position. that should behave in the same manner
    @Test
    public void shouldReturnEmptyPageWhenAskedToReadFromEnd() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadAllPage read = es.readAllForwards(Position.END, 1);

            assertTrue(read.isEnd());
            assertEquals(0, read.getMessages().length);
        }
    }

    // return_events_in_same_order_as_written
    @Test
    public void shouldReturnEventsInSameOrderAsWritten() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadAllPage read = es.readAllForwards(Position.START, messages.length);

            assertEventDataEqual(messages, read.getMessages());
        }
    }

    // be_able_to_read_all_one_by_one_until_end_of_stream
    @Test
    public void shouldBeAbleToReadAllOneByOneUntilEnd() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            List<StreamMessage> all = new ArrayList<>();
            Versionstamp position = Position.START;
            ReadAllPage page;
            boolean atEnd = false;
            while (!atEnd) {
                page = es.readAllForwards(position, 1);
                all.addAll(Arrays.asList(page.getMessages()));
                position = page.getNextPosition();
                atEnd = page.isEnd();
            }
            StreamMessage[] messagesArray = new StreamMessage[all.size()];
            TestHelpers.assertEventDataEqual(messages, all.toArray(messagesArray));
        }
    }

    // be_able_to_read_events_slice_at_time
    @Test
    public void shouldBeAbleToReadEventsPageAtATime() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            List<StreamMessage> all = new ArrayList<>();
            Versionstamp position = Position.START;
            ReadAllPage page;
            boolean atEnd = false;
            while (!atEnd) {
                page = es.readAllForwards(position, 5);
                all.addAll(Arrays.asList(page.getMessages()));
                position = page.getNextPosition();
                atEnd = page.isEnd();
            }

            StreamMessage[] messagesArray = new StreamMessage[all.size()];
            TestHelpers.assertEventDataEqual(messages, all.toArray(messagesArray));
        }
    }

    // return_partial_slice_if_not_enough_events
    @Test
    public void shouldReturnPartialPageIfNotEnoughEvents() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadAllPage read = es.readAllForwards(Position.START, 10);

            assertTrue(read.getMessages().length < 10);
            TestHelpers.assertEventDataEqual(messages, read.getMessages());
        }
    }

    // throw_when_got_int_max_value_as_maxcount
    @Test
    public void shouldThrowWhenMaxCountExceedsMaxReadCount() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readAllForwards(Position.END, EventStoreLayer.MAX_READ_SIZE + 1));
        }
    }


    @Test
    public void readAllForwardTest() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadAllPage read = es.readAllForwards(Position.START, 1);

            assertEquals(1, read.getMessages().length);
            assertFalse(read.isEnd());
            TestHelpers.assertEventDataEqual(messages[0], read.getMessages()[0]);
        }
    }

    @Test
    public void readAllForwardMultipleStreamTest() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);
            es.appendToStream("test-stream2", ExpectedVersion.ANY, messages);

            ReadAllPage read = es.readAllForwards(Position.START, 4);

            assertEquals(4, read.getMessages().length);
            assertTrue(read.isEnd());
            TestHelpers.assertEventDataEqual(ObjectArrays.concat(messages, messages, NewStreamMessage.class), read.getMessages());
        }
    }

    @Test
    public void readAllForwardNextPage() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            // TODO: improve test
            ReadAllPage forwardPage = es.readAllForwards(Position.START, 1);

            assertNotNull(forwardPage);
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));

            ReadAllPage nextPage = forwardPage.readNext();
            assertNotNull(nextPage);
            assertTrue(nextPage.getMessages()[0].getMessageId().toString().contains("2"));
        }
    }

}
