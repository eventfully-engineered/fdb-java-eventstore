package com.seancarroll;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class EventStoreLayerTest {

    @BeforeAll
    public static void clean() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            db.run((Transaction tr) -> {
                try {
                    DirectorySubspace ruscelloSubspace = new DirectoryLayer(true).createOrOpen(tr, Arrays.asList("ruscello")).get();
                    tr.clear(ruscelloSubspace.range());
                } catch (InterruptedException|ExecutionException e) {
                    e.printStackTrace();
                }
                return null;
            });
        }
    }

    @Test
    public void readAllForwardTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace ruscelloSubspace = createRuscelloSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, ruscelloSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadAllPage forwardPage = es.readAllForwards(0, 1);
            assertNotNull(forwardPage);
            assertEquals(1, forwardPage.getMessages().length);
            assertFalse(forwardPage.isEnd());
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));

        }
    }

    @Test
    public void readAllForwardMultipleStreamTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace ruscelloSubspace = createRuscelloSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, ruscelloSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);
            es.appendToStream("test-stream2", ExpectedVersion.ANY, messages);

            ReadAllPage forwardPage = es.readAllForwards(0, 4);
            assertNotNull(forwardPage);
            assertEquals(4, forwardPage.getMessages().length);
            assertFalse(forwardPage.isEnd());
        }
    }

    @Test
    public void readAllBackwardsTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace ruscelloSubspace = createRuscelloSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, ruscelloSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadAllPage backwardPage = es.readAllBackwards(0, 1);

            assertNotNull(backwardPage);
            assertEquals(1, backwardPage.getMessages().length);
            assertFalse(backwardPage.isEnd());
            assertTrue(backwardPage.getMessages()[0].getMessageId().toString().contains("5"));
        }
    }


    @Test
    public void readAllBackwardsMultipleStreamTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace ruscelloSubspace = createRuscelloSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, ruscelloSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);
            es.appendToStream("test-stream2", ExpectedVersion.ANY, messages);

            ReadAllPage forwardPage = es.readAllBackwards(0, 4);

            assertNotNull(forwardPage);
            assertEquals(4, forwardPage.getMessages().length);
            assertFalse(forwardPage.isEnd());

        }
    }

    @Test
    public void readStreamForward() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace ruscelloSubspace = createRuscelloSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, ruscelloSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadStreamPage forwardPage = es.readStreamForwards("test-stream", 0, 1);

            assertNotNull(forwardPage);
            assertEquals(1, forwardPage.getMessages().length);
            assertFalse(forwardPage.isEnd());
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));
        }
    }

    @Test
    public void readStreamBackwards() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace ruscelloSubspace = createRuscelloSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, ruscelloSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadStreamPage backwardPage = es.readStreamBackwards("test-stream", 0, 1);

            assertNotNull(backwardPage);
            assertEquals(1, backwardPage.getMessages().length);
            assertFalse(backwardPage.isEnd());
            assertTrue(backwardPage.getMessages()[0].getMessageId().toString().contains("5"));
        }
    }

    @Test
    public void readHeadPosition() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace ruscelloSubspace = createRuscelloSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, ruscelloSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            long headPosition = db.run((Transaction tr) -> {
                Subspace globalSubspace = ruscelloSubspace.subspace(Tuple.from(EventStoreSubspaces.GLOBAL.getValue()));
                try {
                    // TODO: hmmm....this is a versionstamp. how to store position
                    return es.readHeadPosition(tr, globalSubspace);

                } catch (Exception e) {
                    System.out.println(e);
                    throw new RuntimeException();
                }
            });

            assertEquals(5L, headPosition, 0);

        }
    }

    private static DirectorySubspace createRuscelloSubspace(Database db) {
        return db.run((Transaction tr) -> {
                try {
                    return new DirectoryLayer(true).createOrOpen(tr, Collections.singletonList("ruscello")).get();
                } catch (InterruptedException|ExecutionException e) {
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
            newMessages[i] = new NewStreamMessage(id, "type", jsonData, "\"metadata\"");
        }
        return newMessages;
    }

}
