package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
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

class ReadAllEventsForwardTests extends TestFixture {

    private FDB fdb;

    @BeforeEach
    void clean() throws ExecutionException, InterruptedException {
        fdb = FDB.selectAPIVersion(600);
        TestHelpers.clean(fdb);
    }

    @Test
    void shouldBeAbleToReadLastEvent() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            ReadAllPage read = es.readAllForwards(Position.END, 1).get();

            assertTrue(read.isEnd());
            assertEquals(1, read.getMessages().length);
            TestHelpers.assertEventDataEqual(messages[4], read.getMessages()[0]);
        }
    }

    @Test
    void shouldReturnEventsInSameOrderAsWritten() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            String stream = "test-stream";
            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream(stream, ExpectedVersion.ANY, messages).get();

            ReadAllPage read = es.readAllForwards(Position.START, messages.length).get();

            assertEventDataEqual(messages, read.getMessages());
        }
    }

    @Test
    void shouldBeAbleToReadAllOneByOneUntilEnd() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            List<StreamMessage> all = new ArrayList<>();
            Versionstamp position = Position.START;
            ReadAllPage page;
            boolean atEnd = false;
            while (!atEnd) {
                page = es.readAllForwards(position, 1).get();
                all.addAll(Arrays.asList(page.getMessages()));
                position = page.getNextPosition();
                atEnd = page.isEnd();
            }
            StreamMessage[] messagesArray = new StreamMessage[all.size()];
            TestHelpers.assertEventDataEqual(messages, all.toArray(messagesArray));
        }
    }

    @Test
    void shouldBeAbleToReadEventsPageAtATime() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            List<StreamMessage> all = new ArrayList<>();
            Versionstamp position = Position.START;
            ReadAllPage page;
            boolean atEnd = false;
            while (!atEnd) {
                page = es.readAllForwards(position, 5).get();
                all.addAll(Arrays.asList(page.getMessages()));
                position = page.getNextPosition();
                atEnd = page.isEnd();
            }

            StreamMessage[] messagesArray = new StreamMessage[all.size()];
            TestHelpers.assertEventDataEqual(messages, all.toArray(messagesArray));
        }
    }

    @Test
    void shouldBeAbleToPageViaReadNext() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            ReadAllPage page = es.readAllForwards(Position.START, 1).get();
            List<StreamMessage> all = new ArrayList<>(Arrays.asList(page.getMessages()));
            while (!page.isEnd()) {
                page = page.readNext().get();
                all.addAll(Arrays.asList(page.getMessages()));
            }

            StreamMessage[] messagesArray = new StreamMessage[all.size()];
            TestHelpers.assertEventDataEqual(messages, all.toArray(messagesArray));
        }
    }

    @Test
    void shouldReturnPartialPageIfNotEnoughEvents() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            ReadAllPage read = es.readAllForwards(Position.START, 10).get();

            assertTrue(read.getMessages().length < 10);
            TestHelpers.assertEventDataEqual(messages, read.getMessages());
        }
    }

    @Test
    void shouldThrowWhenMaxCountExceedsMaxReadCount() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get()
                ;

            assertThrows(IllegalArgumentException.class, () -> es.readAllForwards(Position.END, EventStoreLayer.MAX_READ_SIZE + 1));
        }
    }

    @Test
    void readAllForwardMultipleStreamTest() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db).get();

            NewStreamMessage[] messages = createNewStreamMessages(1, 2);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();
            es.appendToStream("test-stream2", ExpectedVersion.ANY, messages).get();

            ReadAllPage read = es.readAllForwards(Position.START, 4).get();

            assertEquals(4, read.getMessages().length);
            assertTrue(read.isEnd());
            TestHelpers.assertEventDataEqual(ObjectArrays.concat(messages, messages, NewStreamMessage.class), read.getMessages());
        }
    }

}
