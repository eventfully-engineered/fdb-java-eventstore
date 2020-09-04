package com.eventfully.foundationdb.eventstore;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.eventfully.foundationdb.eventstore.TestHelpers.assertEventDataEqual;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReadEventStreamFowardTests extends ITFixture {

    @Test
    void shouldThrowWhenCountLessThanOrEqualZero() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", 0, 0));
        }
    }

    @Test
    void shouldThrowWhenStartLessThanNegativeOne() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", -2, 1));
        }
    }

    @Test
    void shouldThrowWhenMaxCountExceedsMaxReadCount() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", 0, EventStoreLayer.MAX_READ_SIZE + 1));
        }
    }

    @Test
    void shouldNotifyUsingStatusCodeWhenStreamNotFound() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            ReadStreamSlice read = es.readStreamForwards("test-stream", 0, 1).get();

            assertEquals(SliceReadStatus.STREAM_NOT_FOUND, read.getStatus());
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
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            // ExpectedVersion.EmptyStream
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadStreamSlice read = es.readStreamForwards(stream, 10, 1).get();

            assertEquals(0, read.getMessages().length);
        }
    }

    @Test
    void shouldReturnPartialSliceWhenNotEnoughEventsInStream() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            // ExpectedVersion.EmptyStream
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadStreamSlice read = es.readStreamForwards(stream, 4, 5).get();

            assertEquals(1, read.getMessages().length);
        }
    }

    @Test
    void shouldReturnEventsInSameOrderAsWritten() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadStreamSlice read = es.readStreamForwards(stream, 0, messages.length).get();

            assertEventDataEqual(messages, read.getMessages());
        }
    }

    @Test
    void shouldBeAbleToReadSingleEventFromArbitraryPosition() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadStreamSlice read = es.readStreamForwards(stream, 4, 1).get();

            assertEventDataEqual(messages[4], read.getMessages()[0]);
        }
    }

    @Test
    void shouldBeAbleToReadSliceFromArbitraryPosition() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadStreamSlice read = es.readStreamForwards(stream, 3, 2).get();

            // TODO: use a comparator something like EventDataComparer
            assertEquals(2, read.getMessages().length);
        }
    }

    @Test
    void shouldBeAbleToReadLastEvent() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadStreamSlice read = es.readStreamForwards(stream, StreamPosition.END, 1).get();

            TestHelpers.assertEventDataEqual(messages[4], read.getMessages()[0]);
        }
    }

    @Test
    void shouldBeAbleToReadAllOneByOneUntilEnd() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            List<StreamMessage> all = new ArrayList<>();
            Long position = StreamPosition.START;
            ReadStreamSlice slice;
            boolean atEnd = false;
            while (!atEnd) {
                slice = es.readStreamForwards("test-stream", position, 1).get();
                all.addAll(Arrays.asList(slice.getMessages()));
                position = slice.getNextStreamVersion();
                atEnd = slice.isEnd();
            }
            StreamMessage[] messagesArray = new StreamMessage[all.size()];
            TestHelpers.assertEventDataEqual(messages, all.toArray(messagesArray));
        }
    }

    @Test
    void shouldBeAbleToReadEventsSliceAtATime() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            List<StreamMessage> all = new ArrayList<>();
            Long position = StreamPosition.START;
            ReadStreamSlice slice;
            boolean atEnd = false;
            while (!atEnd) {
                slice = es.readStreamForwards("test-stream", position, 5).get();
                all.addAll(Arrays.asList(slice.getMessages()));
                position = slice.getNextStreamVersion();
                atEnd = slice.isEnd();
            }

            StreamMessage[] messagesArray = new StreamMessage[all.size()];
            TestHelpers.assertEventDataEqual(messages, all.toArray(messagesArray));
        }
    }

    @Test
    void shouldBeAbleToReadSlicesViaReadNext() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            List<StreamMessage> all = new ArrayList<>();
            ReadStreamSlice slice = es.readStreamForwards("test-stream", StreamPosition.START, 1).get();
            while (slice.getMessages().length > 0 || !slice.isEnd()) {
                all.addAll(Arrays.asList(slice.getMessages()));
                slice = slice.readNext().get();
            }

            StreamMessage[] messagesArray = new StreamMessage[all.size()];
            TestHelpers.assertEventDataEqual(messages, all.toArray(messagesArray));
        }
    }

    // TODO: Can_read_next_page_past_end_of_stream
}
