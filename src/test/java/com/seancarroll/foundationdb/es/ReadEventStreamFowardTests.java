package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.seancarroll.foundationdb.es.TestHelpers.assertEventDataEqual;
import static org.junit.jupiter.api.Assertions.*;

public class ReadEventStreamFowardTests extends TestFixture {

    @BeforeEach
    public void clean() {
        FDB fdb = FDB.selectAPIVersion(520);
        TestHelpers.clean(fdb);
    }

    @Test
    public void shouldThrowWhenCountLessThanOrEqualZero() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", 0, 0));
        }
    }

    @Test
    public void shouldThrowWhenStartLessThanNegativeOne() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", -2, 1));
        }
    }

    @Test
    public void shouldThrowWhenMaxCountExceedsMaxReadCount() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", 0, EventStoreLayer.MAX_READ_SIZE + 1));
        }
    }

    @Test
    public void shouldNotifyUsingStatusCodeWhenStreamNotFound() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            ReadStreamPage read = es.readStreamForwards("test-stream", 0, 1);

            assertEquals(PageReadStatus.STREAM_NOT_FOUND, read.getStatus());
        }
    }


//    @Test
//    public void shouldReturnNoEventsWhenStreamIsEmpty() {
//
//    }

    @Test
    public void shouldNotifyUsingStatusCodeWhenStreamIsDeleted() {
        fail();
    }


    @Test
    public void shouldReturnEmptySliceForNonExistingRange() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            // ExpectedVersion.EmptyStream
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadStreamPage read = es.readStreamForwards(stream, 10, 1);

            assertEquals(0, read.getMessages().length);
        }
    }

    @Test
    public void shouldReturnPartialSliceWhenNotEnoughEventsInStream() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            // ExpectedVersion.EmptyStream
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadStreamPage read = es.readStreamForwards(stream, 4, 5);

            assertEquals(1, read.getMessages().length);
        }
    }

    @Test
    public void shouldReturnEventsInSameOrderAsWritten() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadStreamPage read = es.readStreamForwards(stream, 0, messages.length);
            
            assertEventDataEqual(messages, read.getMessages());
        }
    }

    @Test
    public void shouldBeAbleToReadSingleEventFromArbitraryPosition() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadStreamPage read = es.readStreamForwards(stream, 4, 1);

            assertEventDataEqual(messages[4], read.getMessages()[0]);
        }
    }

    @Test
    public void shouldBeAbleToReadSliceFromArbitraryPosition() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadStreamPage read = es.readStreamForwards(stream, 3, 2);

            // TODO: use a comparator something like EventDataComparer
            assertEquals(2, read.getMessages().length);
        }
    }

    // TODO: improve test
    @Test
    public void readStreamForwardNextPage() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadStreamPage forwardPage = es.readStreamForwards("test-stream", 0, 1);


            assertNotNull(forwardPage);
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));

            ReadStreamPage nextPage = forwardPage.getNext();
            assertNotNull(nextPage);
            assertEquals(1, nextPage.getMessages().length);
            assertFalse(nextPage.isEnd());
            assertTrue(nextPage.getMessages()[0].getMessageId().toString().contains("2"));

        }
    }

    // Can_read_next_page_past_end_of_stream

    @Test
    public void shouldBeAbleToReadLastEvent() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadStreamPage read = es.readStreamForwards(stream, StreamPosition.END, 1);

            TestHelpers.assertEventDataEqual(messages[4], read.getMessages()[0]);
        }
    }

    @Test
    public void shouldBeAbleToReadAllOneByOneUntilEnd() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            List<StreamMessage> all = new ArrayList<>();
            Long position = StreamPosition.START;
            ReadStreamPage page;
            boolean atEnd = false;
            while (!atEnd) {
                page = es.readStreamForwards("test-stream", position, 1);
                all.addAll(Arrays.asList(page.getMessages()));
                position = page.getNextStreamVersion();
                atEnd = page.isEnd();
            }
            StreamMessage[] messagesArray = new StreamMessage[all.size()];
            TestHelpers.assertEventDataEqual(messages, all.toArray(messagesArray));
        }
    }

    @Test
    public void shouldBeAbleToReadEventsPageAtATime() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            List<StreamMessage> all = new ArrayList<>();
            Long position = StreamPosition.START;
            ReadStreamPage page;
            boolean atEnd = false;
            while (!atEnd) {
                page = es.readStreamForwards("test-stream", position, 5);
                all.addAll(Arrays.asList(page.getMessages()));
                position = page.getNextStreamVersion();
                atEnd = page.isEnd();
            }

            StreamMessage[] messagesArray = new StreamMessage[all.size()];
            TestHelpers.assertEventDataEqual(messages, all.toArray(messagesArray));
        }
    }


    // shouldReturnEmptyPageWhenAskedToReadFromEnd
}
