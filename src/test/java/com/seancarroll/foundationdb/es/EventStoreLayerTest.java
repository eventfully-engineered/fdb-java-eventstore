package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

// TODO: clean up tests
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

    @Test
    public void versionTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadAllPage forwardPage = es.readAllForwards(0, 2);
            assertNotNull(forwardPage);
            assertEquals(2, forwardPage.getMessages().length);
            assertFalse(forwardPage.isEnd());
            assertEquals("type", forwardPage.getMessages()[0].getType());
            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));

        }
    }

//    @Test
//    public void versionTest2() {
//        FDB fdb = FDB.selectAPIVersion(520);
//        try (Database db = fdb.open()) {
//            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
//            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);
//
//            NewStreamMessage[] messages = createNewStreamMessages(1);
//            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);
//
//            ReadAllPage forwardPage = es.readAllForwards(0, 1);
//            assertNotNull(forwardPage);
//            assertEquals(1, forwardPage.getMessages().length);
//            assertFalse(forwardPage.isEnd());
//            assertEquals("type", forwardPage.getMessages()[0].getType());
//            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));
//            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));
//            System.out.println(forwardPage.getMessages()[0].getPosition());
//
//            ReadStreamPage streamPage = es.readStreamForwards("test-stream", 0, 1);
//            System.out.println(streamPage.getMessages()[0].getPosition());
//
//        }
//    }


    @Test
    public void readAllForwardTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            ReadAllPage forwardPage = es.readAllForwards(0, 1);
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

            ReadAllPage forwardPage = es.readAllForwards(0, 4);
            assertNotNull(forwardPage);
            assertEquals(4, forwardPage.getMessages().length);
            assertFalse(forwardPage.isEnd());
            assertEquals("type", forwardPage.getMessages()[0].getType());
            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));
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

            ReadAllPage backwardPage = es.readAllBackwards(0, 1);

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

            ReadAllPage forwardPage = es.readAllBackwards(0, 4);

            assertNotNull(forwardPage);
            assertEquals(4, forwardPage.getMessages().length);
            assertFalse(forwardPage.isEnd());
            assertEquals("type", forwardPage.getMessages()[0].getType());
            assertTrue(new String(forwardPage.getMessages()[0].getMetadata()).contains("metadata"));

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
        }
    }

    @Test
    public void readHeadPositionNoStreams() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            StreamId streamId = new StreamId("non-existext-stream");

            Long headPosition = db.run((Transaction tr) -> {
                Subspace streamSubspace = eventStoreSubspace.subspace(Tuple.from(EventStoreSubspaces.STREAM.getValue(), streamId.getHash()));
                try {
                    return es.readHeadPosition(tr, streamSubspace);
                } catch (Exception e) {
                    System.out.println(e);
                    throw new RuntimeException();
                }
            });

            assertEquals(0L, headPosition.longValue());

        }
    }

    @Test
    public void readHeadPosition() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            StreamId streamId = new StreamId("test-stream");

            long headPosition = db.run((Transaction tr) -> {
                Subspace streamSubspace = eventStoreSubspace.subspace(Tuple.from(EventStoreSubspaces.STREAM.getValue(), streamId.getHash()));
                try {
                    return es.readHeadPosition(tr, streamSubspace);
                } catch (Exception e) {
                    System.out.println(e);
                    throw new RuntimeException();
                }
            });

            assertEquals(4L, headPosition);
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


//    @Test
//    public void compareReadingHeadPosition() {
//        FDB fdb = FDB.selectAPIVersion(520);
//        try (Database db = fdb.open()) {
//            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
//            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);
//
//            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
//            es.appendToStream("test-stream", ExpectedVersion.ANY, messages);
//
//            Versionstamp headPosition = db.run((Transaction tr) -> {
//                Subspace globalSubspace = eventStoreSubspace.subspace(Tuple.from(EventStoreSubspaces.GLOBAL.getValue()));
//                try {
//                    // TODO: hmmm....this is a versionstamp. how to store position
//                    Versionstamp hp1 = es.readHeadPosition(tr, globalSubspace);
//                    Versionstamp hp2 = Versionstamp.fromBytes(tr.getVersionstamp().get());
//
//                    int comparison = hp1.compareTo(hp2);
//                } catch (Exception e) {
//                    System.out.println(e);
//                    throw new RuntimeException();
//                }
//
//                return null;
//            });
//
//
//        }
//    }

}
