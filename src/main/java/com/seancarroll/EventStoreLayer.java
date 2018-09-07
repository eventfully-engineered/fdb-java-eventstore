package com.seancarroll;

//import com.foundationdb.directory.DirectoryLayer;
//import com.foundationdb.directory.DirectorySubspace;
//import com.foundationdb.qp.operator.RowCursor;
//import com.foundationdb.qp.row.Row;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.uuid.Generators;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;


// https://eventstore.org/docs/dotnet-api/reading-events/index.html
// https://github.com/apple/foundationdb/blob/master/design/tuple.md
// https://forums.foundationdb.org/t/application-design-using-subspace-and-tuple/452
// https://forums.foundationdb.org/t/implementing-versionstamps-in-bindings/250
// https://www.snowflake.com/how-foundationdb-powers-snowflake-metadata-forward/
// https://forums.foundationdb.org/t/log-abstraction-on-foundationdb/117/3
// https://github.com/apple/foundationdb/issues/627
// TODO: should be closeable?
// https://github.com/bitgn/layers/blob/beb4429b9015e4c10a03cc147662f0e047491d12/go/eventstore/fdbStore.go
// fdbStore maintains two subspaces:
// Global / [versionstamp] / contract / <- vs pointer
// Aggregate / id / version / contract /
// Mine
// Global / [versionstamp] / stream message (id, type, content, message metadata, etc) / <- vs pointer
// Stream / id (stream hash) / version / stream message (id, type, content, message metadata, etc) /
// do we want contract aka type to be a subspace or part of the value tuple?
// FoundationDB version timestamp doesnt appear to work. likely because we only get one versionstamp per transaction
// could we use the 2 byte user/client portion? take the index of each message as the user bytes portion.
// If we did that we could only support arrays length up to a short (32,767)
// TODO: where should we store stream metadata?
// TODO: whats a common pattern for FoundationDB layers?
// - Should you have clients pass in a transaction?
// - Should our operations create their own trasnaction? If so how can clients make sure everything is one atomic transaction?
// - Should clients pass in their on directory/subspace?
// - How do we want to handle position in global vs stream supspace?
public class EventStoreLayer implements EventStore {

    private static final Logger LOG = LoggerFactory.getLogger(EventStoreLayer.class);

    private static final List<String> GLOBAL_SUBPATH = Arrays.asList(EventStoreSubspaces.GLOBAL.getValue());
    private static final List<String> STREAM_SUBPATH = Arrays.asList(EventStoreSubspaces.STREAM.getValue());

    private final Database database;
    private Directory directory;
    private DirectorySubspace directorySubspace;
    private DirectoryLayer directoryLayer;


    // instead of subspace should we pass in a string which represents the default content subspace aka prefix
    // DirectoryLayer.getDefault() uses DEFAULT_CONTENT_SUBSPACE which is no prefix
    // or we could take in a tuple
    // directorysubspace must allow manual prefixes
    public EventStoreLayer(Database database, DirectorySubspace subspace) {
        this.database = database;
        this.directorySubspace = subspace;
    }

    @Override
    public AppendResult appendToStream(String streamId, int expectedVersion, NewStreamMessage[] messages) {

        HashCode streamHash = Hashing.murmur3_128().hashString(streamId, UTF_8);
        Tuple.from(streamHash + "@" + expectedVersion).pack();

        // TODO: query last tuple in stream to get latest version
        // TODO: how does foundationdb handle conflicting keys, optimistic/pessimistic locking

        int currentVersion = expectedVersion;
        return database.run(tr -> {
            try {

                // TODO: should we have a create/build db method that way we can just call open?
                // What is the overhead of the create check?
                // can we create directories/subspaces via cli?
                //DirectorySubspace globalSubspace = DirectoryLayer.getDefault().createOrOpen(tr, GLOBAL_SUBPATH).get();
                Subspace globalSubspace = directorySubspace.subspace(Tuple.from(EventStoreSubspaces.GLOBAL.getValue()));


                // DirectoryLayer.getDefault() does not allow manual prefixes
                //DirectorySubspace streamSubspace = new DirectoryLayer().createOrOpen(tr, STREAM_SUBPATH).get();
                // Subspace streamSubspace = directorySubspace.subspace(Tuple.from(EventStoreSubspaces.STREAM.toString()));
                Subspace streamSubspace = directorySubspace.subspace(Tuple.from(EventStoreSubspaces.STREAM.getValue(), streamHash.toString()));


                // TODO: what to actually call this...inner stream doesn't really work
                // streamSubspace.subspace(Tuple.from(streamHash.toString()));

                // we are getting them in parallel
                // aggregRecord := GetLastKeyFuture(tr, aggregSpace)
                CompletableFuture<byte[]> streamRecord = getLastKeyFuture(tr, streamSubspace);

                long nextStreamIndex = mustGetNextIndex(streamRecord, streamSubspace, 0);
                //nextAggregIndex := aggregRecord.MustGetNextIndex(0)

                for (int i = 0; i < messages.length; i++) {
                // for (NewStreamMessage message : messages) {
//                    aggregIndex := nextAggregIndex + i
//
//                    contract, data := evt.Payload()
//
//                    tr.Set(globalSpace.Sub(uuid, i, contract), data)
//                    tr.Set(aggregSpace.Sub(aggregIndex, contract), data)

                    long streamIndex = nextStreamIndex + i;

                    // TODO: how should we store metadata
                    //byte[] key = globalSubspace.packWithVersionstamp(Tuple.from(Versionstamp.incomplete()));
                    NewStreamMessage message = messages[i];
                    byte[] value = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getJsonData()).pack();
                    // tr.set(key, value);

                    Tuple t = Tuple.from(Versionstamp.incomplete(i));
                    //tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, t.packWithVersionstamp(), value);
                    // tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.pack(t.packWithVersionstamp()), value);
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(t), value);


                    //byte[] streamKey = streamSubspace.subspace(Tuple.from(streamHash.toString())).pack();
                    tr.set(streamSubspace.subspace(Tuple.from(streamIndex)).pack(), value);
                }


                // TODO: may not need this. Might be able to use foundationdbs versionstamped key
                UUID uuid = Generators.timeBasedGenerator().generate();

                // tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY);

//                CompletableFuture<byte[]> trVersionFuture = db.run((Transaction tr) -> {
//                    // The incomplete Versionstamp will be overwritten with tr's version information when committed.
//                    Tuple t = Tuple.from("prefix", Versionstamp.incomplete());
//                    tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, t.packWithVersionstamp(), new byte[0]);
//                    return tr.getVersionstamp();
//                });
//
//                byte[] trVersion = trVersionFuture.get();
//
//                Versionstamp v = db.run((Transaction tr) -> {
//                    Subspace subspace = new Subspace(Tuple.from("prefix"));
//                    byte[] serialized = tr.getRange(subspace.range(), 1).iterator().next().getKey();
//                    Tuple t = subspace.unpack(serialized);
//                    return t.getVersionstamp(0);
//                });
//
//                assert v.equals(Versionstamp.complete(trVersion));


                // tr.set(subspace.subspace(uuid, i, contract), data)

                // TODO: fix
                return new AppendResult(0, 0L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }


            return null;
        });

    }

    public CompletableFuture<byte[]> getLastKeyFuture(Transaction tr, Subspace subspace) {
        return tr.getKey(KeySelector.lastLessThan(subspace.range().end));
    }


    public long mustGetNextIndex(CompletableFuture<byte[]> key, Subspace subspace, int position) throws ExecutionException, InterruptedException {
        byte[] k = key.get();

        if (ByteBuffer.wrap(k).compareTo(ByteBuffer.wrap(subspace.range().begin)) < 0) {
            return 0;
        }

        Tuple t = subspace.unpack(k);
        if (t == null) {
            throw new RuntimeException("failed to unpack key");
        }

        return t.getLong(0) + 1;
    }

    @Override
    public void deleteStream(String streamId, int expectedVersion) {
        database.run(tr -> {
            return null;
        });
    }

    @Override
    public void deleteMessage(String streamId, UUID messageId) {
        database.run(tr -> {
            return null;
        });
    }

    @Override
    public SetStreamMetadataResult setStreamMetadata(String streamId, int expectedStreamMetadataVersion, Integer maxAge, Integer maxCount, String metadataJson) {
        return null;
    }

    @Override
    public ReadAllPage readAllForwards(long fromPositionInclusive, int maxCount) {
        return readAllInternal(fromPositionInclusive, maxCount, false);
    }

    @Override
    public ReadAllPage readAllBackwards(long fromPositionInclusive, int maxCount) {
        return readAllInternal(fromPositionInclusive, maxCount, true);
    }

    private ReadAllPage readAllInternal(long fromPositionInclusive, int maxCount, boolean reverse) {
        return database.read(tr -> {
            Subspace globalSubspace = directorySubspace.subspace(Tuple.from(EventStoreSubspaces.GLOBAL.getValue()));

            // byte[] start = ByteBuffer.allocate(4).putInt(fromPositionInclusive).array();
            // byte[] end = ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array();

            // TODO: look at the various streaming modes to determine best fit
            //AsyncIterable<KeyValue> r = tr.getRange(new Range(start, end), maxCount, false, StreamingMode.EXACT);
            AsyncIterable<KeyValue> r = tr.getRange(globalSubspace.range(), maxCount, reverse, StreamingMode.EXACT);

            // TODO: how to get a slice?
            try {
                List<KeyValue> kvs = r.asList().get();

                StreamMessage[] messages = new StreamMessage[kvs.size()];
                for (int i = 0; i < kvs.size(); i++) {
                    // for (KeyValue kv : kvs) {
                    byte[] key = kvs.get(i).getKey();
                    LOG.info("key: {}", key);
                    Tuple t = globalSubspace.unpack(key);

                    LOG.info("tuple {}", t);
                    LOG.info("size of tuple: {}", t.size());
                    LOG.info("value {}", kvs.get(i).getValue());
                    Tuple tupleValue = Tuple.fromBytes(kvs.get(i).getValue());

                    // TODO: how to handle streamId, messageId, stream version, position, etc...
                    StreamMessage message = new StreamMessage(
                        tupleValue.getString(1),
                        tupleValue.getUUID(0),
                        0,
                        0L,
                        DateTime.now(),
                        tupleValue.getString(2),
                        "",
                        tupleValue.getString(2)
                    );
                    messages[i] = message;
                }

                ReadDirection direction = reverse ? ReadDirection.BACKWARD : ReadDirection.FORWARD;

                return new ReadAllPage(
                    fromPositionInclusive,
                    0L,
                    false,
                    direction,
                    null,
                    messages);
            } catch (InterruptedException|ExecutionException e) {
                e.printStackTrace();
            }

            return null;
        });
    }

    @Override
    public ReadStreamPage readStreamForwards(String streamId, int fromVersionInclusive, int maxCount) {
        return readStreamInternal(streamId, fromVersionInclusive, maxCount, false);
    }

    @Override
    public ReadStreamPage readStreamBackwards(String streamId, int fromVersionInclusive, int maxCount) {
        return readStreamInternal(streamId, fromVersionInclusive, maxCount, true);
    }

    private ReadStreamPage readStreamInternal(String streamId, int fromVersionInclusive, int maxCount, boolean reverse) {
        HashCode streamHash = Hashing.murmur3_128().hashString(streamId, UTF_8);
        return database.read(tr -> {
            Subspace streamSubspace = directorySubspace.subspace(Tuple.from(EventStoreSubspaces.STREAM.getValue(), streamHash.toString()));

            byte[] start = ByteBuffer.allocate(4).putInt(fromVersionInclusive).array();
            byte[] end = ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array();

            // TODO: look at the various streaming modes to determine best fit
            //AsyncIterable<KeyValue> r = tr.getRange(new Range(start, end), maxCount, false, StreamingMode.EXACT);
            AsyncIterable<KeyValue> r = tr.getRange(streamSubspace.range(), maxCount, reverse, StreamingMode.EXACT);

            // TODO: how to get a slice?
            try {
                List<KeyValue> kvs = r.asList().get();

//                String streamId,
//                PageReadStatus status,
//                int fromStreamVersion,
//                int nextStreamVersion,
//                int lastStreamVersion,
//                long lastStreamPosition,
//                ReadDirection readDirection,
//                boolean isEnd,
//                ReadNextStreamPage readNext


                StreamMessage[] messages = new StreamMessage[kvs.size()];
                for (int i = 0; i < kvs.size(); i++) {
                    // for (KeyValue kv : kvs) {
                    byte[] key = kvs.get(i).getKey();
                    LOG.info("key {}", key);
                    Tuple t = streamSubspace.unpack(key);
                    Tuple tupleValue = Tuple.fromBytes(kvs.get(i).getValue());

                    LOG.info("tuple {}", t);
                    LOG.info("size of tuple: {}", t.size());
                    LOG.info("first tuple is a long: {}", t.getLong(0));
                    LOG.info("value ", kvs.get(i).getValue());
                    StreamMessage message = new StreamMessage(
                        streamId,
                        tupleValue.getUUID(0),
                        0,
                        0L,
                        DateTime.now(),
                        "type",
                        "",
                        new String(kvs.get(i).getValue())
                    );
                    messages[i] = message;
                }

                ReadDirection direction = reverse ? ReadDirection.BACKWARD : ReadDirection.FORWARD;

                return new ReadStreamPage(
                    streamId,
                    PageReadStatus.SUCCESS,
                    fromVersionInclusive,
                    0,
                    0,
                    0L,
                    direction,
                    false,
                    null,
                    messages);
            } catch (InterruptedException|ExecutionException e) {
                e.printStackTrace();
            }

            return null;
        });
    }

    // this might be the same as getLastKeyFuture
    // I dont really like passing in any subsapce and would like to constrain it if possible to ruscello subspaces
    // also dont know if we need to support getting head from both global and stream but it seems like a decent idea
    @Override
    public Long readHeadPosition(Transaction tr, Subspace subspace) throws ExecutionException, InterruptedException {

        byte[] k = tr.getKey(KeySelector.lastLessThan(subspace.range().end)).get();

        if (ByteBuffer.wrap(k).compareTo(ByteBuffer.wrap(subspace.range().begin)) < 0) {
            return 0L;
        }

        Tuple t = subspace.unpack(k);
        if (t == null) {
            throw new RuntimeException("failed to unpack key");
        }

        LOG.info("versionstamp {}", t.getVersionstamp(0).toString());
        LOG.info("versionstamp bytes {}", t.getVersionstamp(0).getBytes());
        LOG.info("versionstamp as long {}", ByteBuffer.wrap(t.getVersionstamp(0).getBytes()).getLong());
        LOG.info("committedversion {}", tr.getCommittedVersion());
        return t.getLong(0) + 1;
    }

    @Override
    public StreamMetadataResult getStreamMetadata(String streamId) {
        return null;
    }
}
