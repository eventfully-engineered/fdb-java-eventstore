package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;

// use Long.parseUnsignedLong
// https://github.com/jaytaylor/sql-layer
// https://apple.github.io/foundationdb/developer-guide.html#namespace-management
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
// - Should our operations create their own transaction? If so how can clients make sure everything is one atomic transaction?
// - Should clients pass in their on directory/subspace?
// - How do we want to handle position in global vs stream subspace?
public class EventStoreLayer implements EventStore {

    private static final Logger LOG = LoggerFactory.getLogger(EventStoreLayer.class);

    private final Database database;
    private final DirectorySubspace esSubspace;


    // instead of subspace should we pass in a string which represents the default content subspace aka prefix
    // DirectoryLayer.getDefault() uses DEFAULT_CONTENT_SUBSPACE which is no prefix
    // or we could take in a tuple
    // directorysubspace must allow manual prefixes
    public EventStoreLayer(Database database, DirectorySubspace subspace) {
        this.database = database;
        this.esSubspace = subspace;
    }

    @Override
    public AppendResult appendToStream(String streamId, int expectedVersion, NewStreamMessage[] messages) {
        HashCode streamHash = createHash(streamId);

        // TODO: query last tuple in stream to get latest version
        // TODO: how does foundationdb handle conflicting keys, optimistic/pessimistic locking

        int currentVersion = expectedVersion;
        return database.run(tr -> {
            try {
                // TODO: should we have a create/build db method that way we can just call open?
                // What is the overhead of the create check?
                // can we create directories/subspaces via cli?
                Subspace globalSubspace = esSubspace.subspace(Tuple.from(EventStoreSubspaces.GLOBAL.getValue()));
                Subspace streamSubspace = esSubspace.subspace(Tuple.from(EventStoreSubspaces.STREAM.getValue(), streamHash.toString()));

//                long readPosition = readHeadPosition(tr, streamSubspace);
                ReadStreamPage backwardPage = readStreamBackwards(streamId, 0, 1);

                // TODO: can we remove position from page? I dont think position is what we are looking for.
                // or this could be the versionstamp from the global stream? not sure if thats useful?
                // TODO: change to backwardPage.getNextStreamVersion() or maybe backwardPage.getLastStreamVersion()
                long streamPosition = backwardPage.getMessages().length == 0
                    ? 0
                    : backwardPage.getMessages()[0].getPosition();

                for (int i = 0; i < messages.length; i++) {
                    // TODO: make this an atomic operation via MutationType
                    long streamIndex = streamPosition + i;

                    Tuple t = Tuple.from(Versionstamp.incomplete(i));
                    byte[] vs = globalSubspace.packWithVersionstamp(t);

                    // TODO: how should we store metadata
                    NewStreamMessage message = messages[i];
                    byte[] value = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getData(), message.getMetadata()).pack();

                    // TODO: could this also be used in the tuple above? Would we get the same value?
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, vs, value);
                    tr.set(streamSubspace.subspace(Tuple.from(streamIndex)).pack(), value);
                    // getting invalid API call when attempting this
                    // tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(streamIndex)).pack(), value);
                }

                return new AppendResult(0, 0L);
            } catch (Exception e) {
                // TODO: what to actually do here
                LOG.error("error appending to stream", e);
            }

            return null;
        });

    }

    @Override
    public void deleteStream(String streamId, int expectedVersion) {
        database.run(tr -> null);
    }

    @Override
    public void deleteMessage(String streamId, UUID messageId) {
        database.run(tr -> null);
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
            Subspace globalSubspace = esSubspace.subspace(Tuple.from(EventStoreSubspaces.GLOBAL.getValue()));

            // TODO: look at the various streaming modes to determine best fit
            AsyncIterable<KeyValue> r = tr.getRange(globalSubspace.range(), maxCount, reverse);
            try {
                ReadDirection direction = reverse ? ReadDirection.BACKWARD : ReadDirection.FORWARD;

                List<KeyValue> kvs = r.asList().get();
                if (kvs.isEmpty()) {
                    return new ReadAllPage(
                        fromPositionInclusive,
                        0L,
                        false,
                        direction,
                        null,
                        new StreamMessage[0]);
                }

                StreamMessage[] messages = new StreamMessage[kvs.size()];
                for (int i = 0; i < kvs.size(); i++) {
                    byte[] key = kvs.get(i).getKey();
                    Tuple t = globalSubspace.unpack(key);
                    LOG.info("readAllInternal key: {}", t.get(0));
                    Tuple tupleValue = Tuple.fromBytes(kvs.get(i).getValue());

                    // TODO: how to handle streamId, messageId, stream version, position, etc...
                    StreamMessage message = new StreamMessage(
                        tupleValue.getString(1),
                        tupleValue.getUUID(0),
                        0,
                        0L,
                        DateTime.now(),
                        tupleValue.getString(2),
                        tupleValue.getBytes(4),
                        tupleValue.getBytes(3)
                    );
                    messages[i] = message;

                   // LOG.info("readAllInternal value versionstamp {}", tupleValue.getVersionstamp(5));
                }

                return new ReadAllPage(
                    fromPositionInclusive,
                    0L,
                    false,
                    direction,
                    null,
                    messages);
            } catch (InterruptedException|ExecutionException e) {
                // TODO: what do we actually want to do here
                LOG.error("error reading from global subspace", e);
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
        HashCode streamHash = createHash(streamId);
        return database.read(tr -> {
            Subspace streamSubspace = esSubspace.subspace(Tuple.from(EventStoreSubspaces.STREAM.getValue(), streamHash.toString()));
            // TODO: look at the various streaming modes to determine best fit.
            AsyncIterable<KeyValue> r = tr.getRange(streamSubspace.range(), maxCount, reverse, StreamingMode.WANT_ALL);

            try {

                ReadDirection direction = reverse ? ReadDirection.BACKWARD : ReadDirection.FORWARD;

                List<KeyValue> kvs = r.asList().get();

                if (kvs.isEmpty()) {
                    return new ReadStreamPage(
                        streamId,
                        PageReadStatus.STREAM_NOT_FOUND,
                        fromVersionInclusive,
                        StreamVersion.END,
                        StreamVersion.END,
                        Position.END,
                        direction,
                        true,
                        null,
                        new StreamMessage[0]);
                }

                StreamMessage[] messages = new StreamMessage[kvs.size()];
                for (int i = 0; i < kvs.size(); i++) {
                    byte[] key = kvs.get(i).getKey();
                    Tuple t = streamSubspace.unpack(key);
                    Tuple tupleValue = Tuple.fromBytes(kvs.get(i).getValue());

                    StreamMessage message = new StreamMessage(
                        streamId,
                        tupleValue.getUUID(0),
                        0,
                        0L,
                        DateTime.now(),
                        tupleValue.getString(2),
                        tupleValue.getBytes(4),
                        tupleValue.getBytes(3)
                    );
                    messages[i] = message;

                    //LOG.info("readStreamInternal value versionstamp {}", tupleValue.getVersionstamp(5));
                }

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
                // TODO: what do we actually want to do here
                LOG.error("error reading from stream {}", streamId, e);
            }

            return null;
        });
    }

    // TODO: Need to be consistent with what we call "position" vs what we call "version"
    // I dont really like passing in any subsapce and would like to constrain it if possible to es subspaces
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

        return t.getLong(0);
    }

    @Override
    public StreamMetadataResult getStreamMetadata(String streamId) {
        return null;
    }

    private static HashCode createHash(String streamId) {
        return Hashing.murmur3_128().hashString(streamId, UTF_8);
    }
}
