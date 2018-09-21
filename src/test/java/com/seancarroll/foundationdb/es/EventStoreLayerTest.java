package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

// TODO: clean up tests
// I think it might make sense to split this into separate test files per method
public class EventStoreLayerTest {

    @BeforeEach
    public void clean() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            db.run((Transaction tr) -> {
                DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
                tr.clear(eventStoreSubspace.range());
                return null;
            });
        }
    }


    // TODO: add test where we maxCount for stream is less than total
    // TODO: add test where we maxCount for stream is more than total
    // TODO: add test for expected stream none
    // TODO: add test for wrong version expected with expected stream none aka writing it multiple times
    // TODO: add test for expected stream any with stream doesnt exist
    // TODO: add test for expected stream any with stream with at least 1 message  (version > 0)
    // TODO: add test for idempotency check for expected version any
    // TODO: add test for expected version
    // TODO: add test for expected version where stream does not yet exist
    // TODO: add test for expected version where stream exists and expected version is end (valid?)
    // TODO: add test for expected version where there is at least one message (version > 0)
    // TODO: add test for wrong version expected for expected version


    @Test
    public void versionTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadAllPage forwardPage = es.readAllForwards(Position.START, 2);
            assertNotNull(forwardPage);
            assertEquals(2, forwardPage.getMessages().length);
            assertTrue(forwardPage.isEnd());
            assertEquals("type", forwardPage.getMessages()[0].getType());
            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));

        }
    }

    @Test
    public void readAllForwardTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadAllPage forwardPage = es.readAllForwards(Position.START, 1);
            assertNotNull(forwardPage);
            assertEquals(1, forwardPage.getMessages().length);
            assertFalse(forwardPage.isEnd());
            assertEquals("type", forwardPage.getMessages()[0].getType());
            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));
        }
    }

    @Test
    public void readAllForwardMultipleStreamTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);
            es.appendToStream("test-stream2", ExpectedVersion.ANY, messages);

            ReadAllPage forwardPage = es.readAllForwards(Position.START, 4);
            assertNotNull(forwardPage);
            assertEquals(4, forwardPage.getMessages().length);
            assertTrue(forwardPage.isEnd());
            assertEquals("type", forwardPage.getMessages()[0].getType());
            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));
        }
    }

    @Test
    public void readAllForwardNextPage() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            // TODO: improve test
            ReadAllPage forwardPage = es.readAllForwards(Position.START, 1);
            assertNotNull(forwardPage);
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));

            ReadAllPage nextPage = forwardPage.readNext();
            assertNotNull(nextPage);
            assertTrue(nextPage.getMessages()[0].getMessageId().toString().contains("2"));
        }
    }

    @Test
    public void readAllBackwardsTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            // TODO: using Position.START here feels strange. EventStore uses Position.END which I think is more logical
            ReadAllPage backwardPage = es.readAllBackwards(Position.START, 1);

            assertNotNull(backwardPage);
            assertEquals(1, backwardPage.getMessages().length);
            assertFalse(backwardPage.isEnd());
            assertTrue(backwardPage.getMessages()[0].getMessageId().toString().contains("5"));
            assertEquals("type", backwardPage.getMessages()[0].getType());
            assertTrue(new String(backwardPage.getMessages()[0].getMetadata()).contains("metadata"));
        }
    }


    @Test
    public void readAllBackwardsMultipleStreamTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);
            es.appendToStream("test-stream2", ExpectedVersion.ANY, messages);

            ReadAllPage forwardPage = es.readAllBackwards(Position.START, 4);

            assertNotNull(forwardPage);
            assertEquals(4, forwardPage.getMessages().length);
            assertTrue(forwardPage.isEnd());
            assertEquals("type", forwardPage.getMessages()[0].getType());
            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));

        }
    }

    @Test
    public void readAllBackwardNextPage() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            // TODO: improve test
            // Does start make sense here? What does EventStore do?
            ReadAllPage backwardsPage = es.readAllBackwards(Position.START, 1);
            assertNotNull(backwardsPage);
            assertTrue(backwardsPage.getMessages()[0].getMessageId().toString().contains("5"));

            ReadAllPage nextPage = backwardsPage.readNext();
            assertNotNull(nextPage);
            assertTrue(nextPage.getMessages()[0].getMessageId().toString().contains("4"));
        }
    }

    @Test
    public void readStreamForwardStreamNotFoundTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            ReadStreamPage forwardPage = es.readStreamForwards("test-stream", 0, 1);

            assertNotNull(forwardPage);
            assertEquals(0, forwardPage.getMessages().length);
            assertTrue(forwardPage.isEnd());
            assertEquals(PageReadStatus.STREAM_NOT_FOUND, forwardPage.getStatus());
            // TODO: ugh dont like the need to call intValue...Fix
            assertEquals(StreamVersion.END, forwardPage.getNextStreamVersion());
        }
    }


    @Test
    public void readStreamForward() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadStreamPage forwardPage = es.readStreamForwards("test-stream", 0, 1);

            assertNotNull(forwardPage);
            assertEquals(1, forwardPage.getMessages().length);
            assertFalse(forwardPage.isEnd());
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));
            assertEquals("type", forwardPage.getMessages()[0].getType());
            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));
            assertEquals(0, forwardPage.getMessages()[0].getStreamVersion());
            assertEquals(1, forwardPage.getNextStreamVersion());
        }
    }

    @Test
    public void throwWhenMaxCountExceedsMaxReadCount() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", 0, EventStoreLayer.MAX_READ_SIZE + 1));

        }
    }

    @Test
    public void readStreamForwardsShouldThrowIfCountLessThanOrEqualZero() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamForwards("test-stream", 0, 0));
        }
    }

    @Test
    public void readStreamForwardNextPage() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadStreamPage forwardPage = es.readStreamForwards("test-stream", 0, 1);
            assertNotNull(forwardPage);
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));


            // TODO: improve test
            ReadStreamPage nextPage = forwardPage.getNext();
            assertNotNull(nextPage);
            assertEquals(1, nextPage.getMessages().length);
            assertFalse(nextPage.isEnd());
            assertTrue(nextPage.getMessages()[0].getMessageId().toString().contains("2"));

        }
    }

    @Test
    public void readStreamBackwards() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadStreamPage backwardPage = es.readStreamBackwards("test-stream", 0, 1);

            assertNotNull(backwardPage);
            assertEquals(1, backwardPage.getMessages().length);
            assertFalse(backwardPage.isEnd());
            assertTrue(backwardPage.getMessages()[0].getMessageId().toString().contains("5"));
            assertEquals("type", backwardPage.getMessages()[0].getType());
            assertTrue(new String(backwardPage.getMessages()[0].getMetadata()).contains("metadata"));
            assertEquals(4, backwardPage.getMessages()[0].getStreamVersion());
            assertEquals(3, backwardPage.getNextStreamVersion());
        }
    }

    @Test
    public void throwWhenReadBackwardsMaxCountExceedsMaxReadCount() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamBackwards("test-stream", 0, EventStoreLayer.MAX_READ_SIZE + 1));
        }
    }

    @Test
    public void readStreamBackwardsShouldThrowIfCountLessThanOrEqualZero() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            assertThrows(IllegalArgumentException.class, () -> es.readStreamBackwards("test-stream", 0, 0));
        }
    }

    @Test
    public void readStreamBackwardsNextPage() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadStreamPage backwardsPage = es.readStreamBackwards("test-stream", 0, 1);
            assertNotNull(backwardsPage);
            assertTrue(backwardsPage.getMessages()[0].getMessageId().toString().contains("5"));


            // TODO: improve test
            ReadStreamPage nextPage = backwardsPage.getNext();
            assertNotNull(nextPage);
            assertEquals(1, nextPage.getMessages().length);
            assertFalse(nextPage.isEnd());
            assertTrue(nextPage.getMessages()[0].getMessageId().toString().contains("4"));

        }
    }

    @Test
    public void readHeadPosition() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            Long headPosition = es.readHeadPosition();

            assertEquals(0L, headPosition.longValue());
        }
    }

    private static DirectorySubspace createEventStoreSubspace(Database db) {
        return db.run((Transaction tr) -> {
            try {
                return new DirectoryLayer(true).createOrOpen(tr, Collections.singletonList("es")).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    private static NewStreamMessage[] createNewStreamMessages(int... messageNumbers) {
        return createNewStreamMessages("\"data\"", messageNumbers);
    }

    private static NewStreamMessage[] createNewStreamMessages(String jsonData, int[] messageNumbers) {
        NewStreamMessage[] newMessages = new NewStreamMessage[messageNumbers.length];
        for (int i = 0; i < messageNumbers.length; i++) {
            UUID id = UUID.fromString(StringUtils.leftPad("00000000-0000-0000-0000-" + String.valueOf(messageNumbers[i]), 12, "0"));
            newMessages[i] = new NewStreamMessage(id, "type", jsonData.getBytes(), "\"metadata\"".getBytes());
        }
        return newMessages;
    }

}
