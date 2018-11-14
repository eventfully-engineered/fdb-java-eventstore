package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.seancarroll.foundationdb.es.TestHelpers.assertEventDataEqual;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReadEventStreamFowardTests extends TestFixture {

    private FDB fdb;

    @BeforeEach
    void clean() throws ExecutionException, InterruptedException {
        fdb = FDB.selectAPIVersion(520);
        TestHelpers.clean(fdb);
    }

    @Test
    void shouldThrowWhenCountLessThanOrEqualZero() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", 0, 0));
        }
    }

    @Test
    void shouldThrowWhenStartLessThanNegativeOne() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", -2, 1));
        }
    }

    @Test
    void shouldThrowWhenMaxCountExceedsMaxReadCount() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", 0, EventStoreLayer.MAX_READ_SIZE + 1));
        }
    }

    @Test
    void shouldNotifyUsingStatusCodeWhenStreamNotFound() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            ReadStreamPage read = es.readStreamForwards("test-stream", 0, 1);

            assertEquals(PageReadStatus.STREAM_NOT_FOUND, read.getStatus());
        }
    }


//    @Test
//    void shouldReturnNoEventsWhenStreamIsEmpty() {
//
//    }

//    @Test
//    void shouldNotifyUsingStatusCodeWhenStreamIsDeleted() {
//        fail();
//    }


    @Test
    void shouldReturnEmptySliceForNonExistingRange() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            // ExpectedVersion.EmptyStream
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadStreamPage read = es.readStreamForwards(stream, 10, 1);

            assertEquals(0, read.getMessages().length);
        }
    }

    @Test
    void shouldReturnPartialSliceWhenNotEnoughEventsInStream() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            // ExpectedVersion.EmptyStream
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadStreamPage read = es.readStreamForwards(stream, 4, 5);

            assertEquals(1, read.getMessages().length);
        }
    }

    @Test
    void shouldReturnEventsInSameOrderAsWritten() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadStreamPage read = es.readStreamForwards(stream, 0, messages.length);
            
            assertEventDataEqual(messages, read.getMessages());
        }
    }

    @Test
    void shouldBeAbleToReadSingleEventFromArbitraryPosition() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadStreamPage read = es.readStreamForwards(stream, 4, 1);

            assertEventDataEqual(messages[4], read.getMessages()[0]);
        }
    }

    @Test
    void shouldBeAbleToReadSliceFromArbitraryPosition() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadStreamPage read = es.readStreamForwards(stream, 3, 2);

            // TODO: use a comparator something like EventDataComparer
            assertEquals(2, read.getMessages().length);
        }
    }

    @Test
    void shouldBeAbleToReadLastEvent() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages);

            ReadStreamPage read = es.readStreamForwards(stream, StreamPosition.END, 1);

            TestHelpers.assertEventDataEqual(messages[4], read.getMessages()[0]);
        }
    }

    @Test
    void shouldBeAbleToReadAllOneByOneUntilEnd() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

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
    void shouldBeAbleToReadEventsPageAtATime() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

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

    @Test
    void shouldBeAbleToPageViaReadNext() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadStreamPage page = es.readStreamForwards("test-stream", StreamPosition.START, 1);
            List<StreamMessage> all = new ArrayList<>(Arrays.asList(page.getMessages()));
            while (!page.isEnd()) {
                page = page.readNext().get();
                all.addAll(Arrays.asList(page.getMessages()));
            }

            StreamMessage[] messagesArray = new StreamMessage[all.size()];
            TestHelpers.assertEventDataEqual(messages, all.toArray(messagesArray));
        }
    }

    // TODO: Can_read_next_page_past_end_of_stream
}
