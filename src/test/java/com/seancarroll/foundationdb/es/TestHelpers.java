package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

class TestHelpers {

    static void clean(FDB fdb) {
        try (Database db = fdb.open()) {
            db.run((Transaction tr) -> {
                DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
                tr.clear(eventStoreSubspace.range());
                return null;
            });
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

}
