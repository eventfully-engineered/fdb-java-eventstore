package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class POCTest {

    // https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/ReadTransaction.html
    // https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/Transaction.html#getCommittedVersion--
    // https://apple.github.io/foundationdb/javadoc/index.html?com/apple/foundationdb/tuple/Versionstamp.html
    // https://forums.foundationdb.org/t/implementing-versionstamps-in-bindings/250
    // https://groups.google.com/forum/#!topic/foundationdb-user/4sMVac98Q6o
    // abdullin go/eventstore: use versionstamps for the global order: https://github.com/bitgn/layers/commit/ccd5d38f4a864ebdee100f357d5642774c25fbbc

    @Test
    public void a() throws ExecutionException, InterruptedException {
        FDB fdb = FDB.selectAPIVersion(520);
        try(Database db = fdb.open()) {
            CompletableFuture<byte[]> trVersionFuture = db.run((Transaction tr) -> {
                // The incomplete Versionstamp will be overwritten with tr's version information when committed.
                Tuple t = Tuple.from("prefix", Versionstamp.incomplete());
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, t.packWithVersionstamp(), new byte[0]);
                return tr.getVersionstamp();
            });

            byte[] trVersion = trVersionFuture.get();

            Versionstamp v = db.run((Transaction tr) -> {
                Subspace subspace = new Subspace(Tuple.from("prefix"));
                byte[] serialized = tr.getRange(subspace.range(), 1).iterator().next().getKey();
                Tuple t = subspace.unpack(serialized);
                return t.getVersionstamp(0);
            });

            Versionstamp completedVersionstamp = Versionstamp.complete(trVersion);
            assert v.equals(completedVersionstamp);

            // Versionstamp completedVersion = Versionstamp.complete(trVersion);
            // System.out.println(completedVersion.toString());
        }
    }


    @Test
    public void poc() {
        FDB fdb = FDB.selectAPIVersion(520);
        //dbFolder.getRoot().getAbsolutePath()
        try(Database db = fdb.open()) {
            // Run an operation on the database
            CompletableFuture<byte[]> trVersionFuture = db.run(tr -> {

                Tuple t = Tuple.from("hello", Versionstamp.incomplete());
                //tr.set(Tuple.from("hello").pack(), Tuple.from("world").pack());

                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, t.packWithVersionstamp(), Tuple.from("world").pack());
                return tr.getVersionstamp();
            });

            byte[] trVersion = trVersionFuture.get();
            System.out.println(Versionstamp.fromBytes(trVersion).toString());

            // Get the value of 'hello' from the database
            String hello = db.run(tr -> {
                System.out.println("Committed version: " + tr.getCommittedVersion());
                System.out.println("Version timestamp: " + tr.getVersionstamp());
                System.out.println("Read version: " + tr.getReadVersion());
                byte[] result = tr.get(Tuple.from("hello").pack()).join();
                Tuple t = Tuple.fromBytes(result);
                System.out.println(t);
                System.out.println(t.getVersionstamp(0).toString());
                return t.getString(0);

                // System.out.println(t.getVersionstamp(0));
                //return t.getString(0);
            });

            System.out.println("Hello " + hello);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
