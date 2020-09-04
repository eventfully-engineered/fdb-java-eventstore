package com.eventfully.foundationdb.eventstore;

import com.apple.foundationdb.Database;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.eventfully.foundationdb.eventstore.TestHelpers.assertEventDataEqual;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReadEventStreamBackwardTests extends ITFixture {

    @Test
    void shouldThrowWhenCountLessThanOrEqualZero() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            assertThrows(IllegalArgumentException.class, () -> es.readStreamBackwards("test-stream", 0, 0));
        }
    }

    @Test
    void shouldThrowWhenStartLessThanNegativeOne() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            assertThrows(IllegalArgumentException.class, () -> es.readStreamBackwards("test-stream", -2, 1));
        }
    }

    @Test
    void shouldThrowWhenMaxCountExceedsMaxReadCount() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            assertThrows(IllegalArgumentException.class, () -> es.readStreamBackwards("test-stream", 0, EventStoreLayer.MAX_READ_SIZE + 1));
        }
    }

    @Test
    void shouldNotifyUsingStatusCodeWhenStreamNotFound() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            ReadStreamSlice read = es.readStreamBackwards("test-stream", 0, 1).get();

            assertEquals(SliceReadStatus.STREAM_NOT_FOUND, read.getStatus());
        }
    }

//    @Test
//    void shouldReturnNoEventsWhenStreamIsEmpty() {
//
//    }


//    @Test
//    void shouldNotifyUsingStatusCodeWhenStreamIsDeleted() {
//        fail("not implemented");
//    }

    @Test
    void shouldReturnEmptySliceForNonExistingRange() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            // ExpectedVersion.EmptyStream
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadStreamSlice read = es.readStreamBackwards(stream, 10, 1).get();

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

            ReadStreamSlice read = es.readStreamBackwards(stream, 1, 5).get();

            assertEquals(2, read.getMessages().length);
        }
    }

    @Test
    void shouldReturnEventsInReverseOrderComparedToWritten() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadStreamSlice read = es.readStreamBackwards(stream, StreamPosition.END, messages.length).get();

            ArrayUtils.reverse(messages);
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

            ReadStreamSlice read = es.readStreamBackwards(stream, 3, 1).get();

            TestHelpers.assertEventDataEqual(messages[3], read.getMessages()[0]);
        }
    }

    @Test
    void shouldBeAbleToReadSliceFromArbitraryPosition() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadStreamSlice read = es.readStreamBackwards(stream, 3, 2).get();

            // TODO: use a comparator something like EventDataComparer
            assertEquals(2, read.getMessages().length);
        }
    }

    @Test
    void shouldBeAbleToReadFirstEvent() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadStreamSlice read = es.readStreamBackwards(stream, StreamPosition.START, 1).get();

            TestHelpers.assertEventDataEqual(messages[0], read.getMessages()[0]);
        }
    }

    @Test
    void shouldBeAbleToReadLastEvent() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadStreamSlice read = es.readStreamBackwards(stream, StreamPosition.END, 1).get();

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
            Long position = StreamPosition.END;
            ReadStreamSlice slice;
            boolean atEnd = false;
            while (!atEnd) {
                slice = es.readStreamBackwards("test-stream", position, 1).get();
                all.addAll(Arrays.asList(slice.getMessages()));
                position = slice.getNextStreamVersion();
                atEnd = slice.isEnd();
            }

            ArrayUtils.reverse(messages);
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
            Long position = StreamPosition.END;
            ReadStreamSlice slice;
            boolean atEnd = false;
            while (!atEnd) {
                slice = es.readStreamBackwards("test-stream", position, 5).get();
                all.addAll(Arrays.asList(slice.getMessages()));
                position = slice.getNextStreamVersion();
                atEnd = slice.isEnd();
            }

            ArrayUtils.reverse(messages);
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

            ReadStreamSlice slice = es.readStreamBackwards("test-stream", StreamPosition.END, 1).get();
            List<StreamMessage> all = new ArrayList<>(Arrays.asList(slice.getMessages()));
            while (!slice.isEnd()) {
                slice = slice.readNext().get();
                all.addAll(Arrays.asList(slice.getMessages()));
            }

            ArrayUtils.reverse(messages);
            StreamMessage[] messagesArray = new StreamMessage[all.size()];
            TestHelpers.assertEventDataEqual(messages, all.toArray(messagesArray));
        }
    }

    // TODO: Can_read_next_page_past_end_of_stream
}
