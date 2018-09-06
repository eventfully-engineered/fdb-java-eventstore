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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                return null;


            });
        }
    }

    @Test
    public void t() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace ruscelloSubspace =
                db.run((Transaction tr) -> {
                    try {
                        return new DirectoryLayer(true).createOrOpen(tr, Arrays.asList("ruscello")).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    return null;
                });

            EventStoreLayer es = new EventStoreLayer(db, ruscelloSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);

            AppendResult result = es.appendToStream("test-stream", ExpectedVersion.ANY, messages);
            assertNotNull(result);

            ReadStreamPage page = es.readStreamForwards("test-stream", 0, 500, false);

            assertNotNull(page);
            assertEquals(5, page.getMessages().length);
            assertFalse(page.isEnd());

            for (int i = 0; i < page.getMessages().length; i++) {
                assertEquals("test-stream", page.getMessages()[i].getStreamId());
            }

            AppendResult result2 = es.appendToStream("test-stream2", ExpectedVersion.ANY, messages);
            assertNotNull(result2);

            ReadAllPage readAllPage = es.readAllForwards(0, 10, false);
            assertNotNull(readAllPage);
            assertEquals(10, readAllPage.getMessages().length);
            assertFalse(page.isEnd());

            for (int i = 0; i < 5; i++) {
                assertEquals("test-stream", readAllPage.getMessages()[i].getStreamId());
            }

            for (int i = 5; i < 10; i++) {
                assertEquals("test-stream2", readAllPage.getMessages()[i].getStreamId());
            }
        }
    }

    @Test
    public void readAllTest() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace ruscelloSubspace =
                db.run((Transaction tr) -> {
                    try {
                        return new DirectoryLayer(true).createOrOpen(tr, Arrays.asList("ruscello")).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    return null;
                });

            EventStoreLayer es = new EventStoreLayer(db, ruscelloSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);

            AppendResult result = es.appendToStream("test-stream", ExpectedVersion.ANY, messages);
            assertNotNull(result);

            ReadAllPage forwardPage = es.readAllForwards(0, 1, false);

            assertNotNull(forwardPage);
            assertEquals(1, forwardPage.getMessages().length);
            assertFalse(forwardPage.isEnd());
            assertTrue(forwardPage.getMessages()[0].getMessageId().toString().contains("1"));
            System.out.println(forwardPage.getMessages()[0].getMessageId());


            ReadAllPage backwardPage = es.readAllBackwards(0, 1, false);

            assertNotNull(backwardPage);
            assertEquals(1, backwardPage.getMessages().length);
            assertFalse(backwardPage.isEnd());
            assertTrue(backwardPage.getMessages()[0].getMessageId().toString().contains("5"));
            System.out.println(backwardPage.getMessages()[0].getMessageId());
        }
    }

    @Test
    public void readHeadPosition() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace ruscelloSubspace =
                db.run((Transaction tr) -> {
                    try {
                        return new DirectoryLayer(true).createOrOpen(tr, Arrays.asList("ruscello")).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    return null;
                });

            EventStoreLayer es = new EventStoreLayer(db, ruscelloSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);

            AppendResult result = es.appendToStream("test-stream", ExpectedVersion.ANY, messages);
            assertNotNull(result);

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

    public static NewStreamMessage[] createNewStreamMessages(int... messageNumbers) {
        return createNewStreamMessages("\"data\"", messageNumbers);
    }

    public static NewStreamMessage[] createNewStreamMessages(String jsonData, int[] messageNumbers) {
        NewStreamMessage[] newMessages = new NewStreamMessage[messageNumbers.length];
        for (int i = 0; i < messageNumbers.length; i++) {
            UUID id = UUID.fromString(StringUtils.leftPad("00000000-0000-0000-0000-" + String.valueOf(messageNumbers[i]), 12, "0"));
            newMessages[i] = new NewStreamMessage(id, "type", jsonData, "\"metadata\"");
        }
        return newMessages;
    }

}
