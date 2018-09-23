package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

abstract class TestFixture {


    static DirectorySubspace createEventStoreSubspace(Database db) {
        return db.run((Transaction tr) -> {
            try {
                return new DirectoryLayer(true).createOrOpen(tr, Collections.singletonList("es")).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    static NewStreamMessage[] createNewStreamMessages(int... messageNumbers) {
        return createNewStreamMessages("\"data\"", messageNumbers);
    }

    static NewStreamMessage[] createNewStreamMessages(String jsonData, int[] messageNumbers) {
        NewStreamMessage[] newMessages = new NewStreamMessage[messageNumbers.length];
        for (int i = 0; i < messageNumbers.length; i++) {
            UUID id = UUID.fromString(StringUtils.leftPad("00000000-0000-0000-0000-" + String.valueOf(messageNumbers[i]), 12, "0"));
            newMessages[i] = new NewStreamMessage(id, "type", jsonData.getBytes(), "\"metadata\"".getBytes());
        }
        return newMessages;
    }
}
