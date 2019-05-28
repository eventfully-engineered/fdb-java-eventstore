package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.collect.ObjectArrays;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class ReadAllEventsBackwardTests extends TestFixture {

    private FDB fdb;

    @BeforeEach
    void clean() throws ExecutionException, InterruptedException {
        fdb = FDB.selectAPIVersion(610);
        TestHelpers.clean(fdb);
    }

    @Test
    void shouldBeAbleToReadFirstEvent() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            ReadAllSlice read = es.readAllBackwards(Position.START, 1).get();

            assertTrue(read.isEnd());
            assertEquals(1, read.getMessages().length);
            TestHelpers.assertEventDataEqual(messages[0], read.getMessages()[0]);
        }
    }

    @Test
    void shouldReturnEventsInReversedOrderComparedToWritten() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            ReadAllSlice read = es.readAllBackwards(Position.END, messages.length).get();

            ArrayUtils.reverse(messages);
            TestHelpers.assertEventDataEqual(messages, read.getMessages());
        }
    }

    @Test
    void shouldBeAbleToReadAllOneByOneUntilEnd() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            List<StreamMessage> all = new ArrayList<>();
            Versionstamp position = Position.END;
            ReadAllSlice slice;
            boolean atEnd = false;
            while (!atEnd) {
                slice = es.readAllBackwards(position, 1).get();
                all.addAll(Arrays.asList(slice.getMessages()));
                position = slice.getNextPosition();
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
            Versionstamp position = Position.END;
            ReadAllSlice slice;
            boolean atEnd = false;
            while (!atEnd) {
                slice = es.readAllBackwards(position, 5).get();
                all.addAll(Arrays.asList(slice.getMessages()));
                position = slice.getNextPosition();
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

            ReadAllSlice slice = es.readAllBackwards(Position.END, 1).get();
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

    @Test
    void shouldReturnPartialSliceIfNotEnoughEvents() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            ReadAllSlice read = es.readAllBackwards(Position.END, 10).get();

            ArrayUtils.reverse(messages);
            assertTrue(read.getMessages().length < 10);
            TestHelpers.assertEventDataEqual(messages, read.getMessages());
        }
    }

    @Test
    void shouldThrowWhenMaxCountExceedsMaxReadCount() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            assertThrows(IllegalArgumentException.class, () -> es.readAllBackwards(Position.END, EventStoreLayer.MAX_READ_SIZE + 1));
        }
    }

    @Test
    void shouldReadFromMultipleStream() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();
            es.appendToStream("test-stream2", ExpectedVersion.ANY, messages).get();

            ReadAllSlice read = es.readAllBackwards(Position.END, 4).get();

            assertEquals(4, read.getMessages().length);
            assertTrue(read.isEnd());
            NewStreamMessage[] combined = ObjectArrays.concat(messages, messages, NewStreamMessage.class);
            ArrayUtils.reverse(combined);
            TestHelpers.assertEventDataEqual(combined, read.getMessages());
        }
    }

}
