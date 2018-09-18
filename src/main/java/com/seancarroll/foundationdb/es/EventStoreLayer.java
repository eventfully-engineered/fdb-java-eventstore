package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;


// TODO: use Long.parseUnsignedLong
// TODO: should be closeable?
// TODO: where should we store stream metadata?
// TODO: whats a common pattern for FoundationDB layers?
// - Should our operations create their own transaction? If so how can clients make sure everything is one atomic transaction?
// - Should you have clients pass in a transaction?
// - Should clients pass in their on directory/subspace?
// - How do we want to handle position in global vs stream subspace?
// TODO: improve my exception handling code
public class EventStoreLayer implements EventStore {

    private static final Logger LOG = LoggerFactory.getLogger(EventStoreLayer.class);

    public static final int MAX_READ_SIZE = 4096;

    private final Database database;
    private final DirectorySubspace esSubspace;


    // instead of subspace should we pass in a string which represents the default content subspace aka prefix
    // DirectoryLayer.getDefault() uses DEFAULT_CONTENT_SUBSPACE which is no prefix
    // or we could take in a tuple
    // directorysubspace must allow manual prefixes
    /**
     *
     * @param database
     * @param subspace
     */
    public EventStoreLayer(Database database, DirectorySubspace subspace) {
        this.database = database;
        this.esSubspace = subspace;
    }

    @Override
    public AppendResult appendToStream(String streamId, int expectedVersion, NewStreamMessage[] messages) {
        return appendToStreamInternal(streamId, expectedVersion, messages);
    }

    private AppendResult appendToStreamInternal(String streamId, int expectedVersion, NewStreamMessage[] messages) {
        Preconditions.checkNotNull(streamId);

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        if (expectedVersion == ExpectedVersion.ANY) {
            return appendToStreamExpectedVersionAny(streamId, messages);
        }
        if (expectedVersion == ExpectedVersion.NO_STREAM) {
            return appendToStreamExpectedVersionNoStream(streamId, messages);
        }
        return appendToStreamExpectedVersion(streamId, expectedVersion, messages);
    }

    // TODO: Idempotency handling. Check if the Messages have already been written.
    // TODO: clean up
    private AppendResult appendToStreamExpectedVersionAny(String streamId, NewStreamMessage[] messages) {
        HashCode streamHash = createHash(streamId);

        AtomicInteger latestStreamVersion = new AtomicInteger();
        CompletableFuture<byte[]> trVersionFuture = database.run(tr -> {
            try {
                Subspace globalSubspace = getGlobalSubspace();
                Subspace streamSubspace = getStreamSubspace(streamHash.toString());

                ReadStreamPage backwardPage = readStreamBackwards(streamId, 0, 1);
                Integer currentStreamVersion = backwardPage.getMessages().length == 0
                    ? StreamVersion.END
                    : backwardPage.getNextStreamVersion();

                latestStreamVersion.set(currentStreamVersion);
                for (int i = 0; i < messages.length; i++) {
                    latestStreamVersion.incrementAndGet();

                    Versionstamp versionstamp = Versionstamp.incomplete(i);

                    // TODO: how should we store metadata
                    NewStreamMessage message = messages[i];
                    Tuple tv = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getData(), message.getMetadata());

                    tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), tv.pack());
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(latestStreamVersion)).pack(), tv.add(versionstamp).packWithVersionstamp());
                }

                return tr.getVersionstamp();
            } catch (Exception e) {
                // TODO: what to actually do here
                LOG.error("error appending to stream", e);
            }

            return null;
        });

        // TODO: all this is pretty terrible
        try {
            byte[] trVersion = trVersionFuture.get();

            Versionstamp completedVersion = Versionstamp.complete(trVersion, messages.length - 1);
            return new AppendResult(latestStreamVersion.get(), 0L);

        } catch (InterruptedException|ExecutionException e) {
            // TODO: what to actually do here
            LOG.error("error appending to stream", e);
        }

        return null;
    }

    // TODO: Idempotency handling. Check if the Messages have already been written.
    // TODO: clean up
    private AppendResult appendToStreamExpectedVersionNoStream(String streamId, NewStreamMessage[] messages) {
        HashCode streamHash = createHash(streamId);

        AtomicInteger latestStreamVersion = new AtomicInteger();
        CompletableFuture<byte[]> trVersionFuture = database.run(tr -> {
            try {
                Subspace globalSubspace = getGlobalSubspace();
                Subspace streamSubspace = getStreamSubspace(streamHash.toString());

                ReadStreamPage backwardPage = readStreamBackwards(streamId, 0, 1);

                if (PageReadStatus.STREAM_NOT_FOUND != backwardPage.getStatus()) {
                    // ErrorMessages.AppendFailedWrongExpectedVersion
                    // $"Append failed due to WrongExpectedVersion.Stream: {streamId}, Expected version: {expectedVersion}"
                    throw new WrongExpectedVersionException(String.format("Append failed due to wrong expected version. Stream %s. Expected version: %d.", streamId, StreamVersion.NONE));
                }

                latestStreamVersion.set(StreamVersion.END);
                for (int i = 0; i < messages.length; i++) {
                    latestStreamVersion.incrementAndGet();

                    Versionstamp versionstamp = Versionstamp.incomplete(i);

                    // TODO: how should we store metadata
                    NewStreamMessage message = messages[i];
                    Tuple tv = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getData(), message.getMetadata());

                    tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), tv.pack());
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(latestStreamVersion)).pack(), tv.add(versionstamp).packWithVersionstamp());
                }

                return tr.getVersionstamp();
            } catch (Exception e) {
                // TODO: what to actually do here
                LOG.error("error appending to stream", e);
            }

            return null;
        });

        // TODO: all this is pretty terrible
        try {
            byte[] trVersion = trVersionFuture.get();

            Versionstamp completedVersion = Versionstamp.complete(trVersion, messages.length - 1);
            return new AppendResult(latestStreamVersion.get(), 0L);

        } catch (InterruptedException|ExecutionException e) {
            // TODO: what to actually do here
            LOG.error("error appending to stream", e);
        }

        return null;
    }

    // TODO: Idempotency handling. Check if the Messages have already been written.
    // TODO: clean up
    private AppendResult appendToStreamExpectedVersion(String streamId, int expectedVersion, NewStreamMessage[] messages) {
        HashCode streamHash = createHash(streamId);

        AtomicInteger latestStreamVersion = new AtomicInteger();
        CompletableFuture<byte[]> trVersionFuture = database.run(tr -> {
            try {
                Subspace globalSubspace = getGlobalSubspace();
                Subspace streamSubspace = getStreamSubspace(streamHash.toString());

                ReadStreamPage backwardPage = readStreamBackwards(streamId, 0, 1);

                Integer currentStreamVersion = backwardPage.getMessages().length == 0
                    ? StreamVersion.END
                    : backwardPage.getNextStreamVersion();

                if (!Objects.equals(expectedVersion, currentStreamVersion)) {
                    throw new WrongExpectedVersionException(String.format("Append failed due to wrong expected version. Stream %s. Expected version: %d. Current version %d.", streamId, expectedVersion, currentStreamVersion));
                }

                latestStreamVersion.set(currentStreamVersion);
                for (int i = 0; i < messages.length; i++) {
                    latestStreamVersion.incrementAndGet();

                    Versionstamp versionstamp = Versionstamp.incomplete(i);

                    // TODO: how should we store metadata
                    NewStreamMessage message = messages[i];
                    Tuple tv = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getData(), message.getMetadata());

                    tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), tv.pack());
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(latestStreamVersion)).pack(), tv.add(versionstamp).packWithVersionstamp());
                }

                return tr.getVersionstamp();
            } catch (Exception e) {
                // TODO: what to actually do here
                LOG.error("error appending to stream", e);
            }

            return null;
        });

        // TODO: all this is pretty terrible
        try {
            byte[] trVersion = trVersionFuture.get();

            // TODO: this is a bit icky
            Versionstamp completedVersion = Versionstamp.complete(trVersion, messages.length - 1);
            return new AppendResult(latestStreamVersion.get(), 0L);

        } catch (InterruptedException|ExecutionException e) {
            // TODO: what to actually do here
            LOG.error("error appending to stream", e);
        }

        return null;
    }

    @Override
    public void deleteStream(String streamId, int expectedVersion) {
        // database.run(tr -> null);
        throw new RuntimeException("Not implemented exception");
    }

    @Override
    public void deleteMessage(String streamId, UUID messageId) {
        // database.run(tr -> null);
        throw new RuntimeException("Not implemented exception");
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
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        return database.read(tr -> {

            Subspace globalSubspace = getGlobalSubspace();

            // add one so we can determine if we are at the end of the stream
            int rangeCount =  maxCount + 1;

            // TODO: look at the various streaming modes to determine best fit
            AsyncIterable<KeyValue> r = tr.getRange(globalSubspace.range(), rangeCount, reverse);
            try {
                ReadDirection direction = reverse ? ReadDirection.BACKWARD : ReadDirection.FORWARD;

                ReadNextAllPage readNext = (Long nextPosition) -> readAllForwards(nextPosition, maxCount);

                List<KeyValue> kvs = r.asList().get();
                if (kvs.isEmpty()) {
                    return new ReadAllPage(
                        fromPositionInclusive,
                        fromPositionInclusive,
                        true,
                        direction,
                        readNext,
                        Empty.STREAM_MESSAGES);
                }

                int limit = Math.min(maxCount, kvs.size());
                StreamMessage[] messages = new StreamMessage[limit];
                for (int i = 0; i < limit; i++) {
                    Tuple tupleValue = Tuple.fromBytes(kvs.get(i).getValue());

                    // TODO: how to handle streamId, messageId, stream version, position, etc...
                    StreamMessage message = new StreamMessage(
                        tupleValue.getString(1),
                        tupleValue.getUUID(0),
                        0, // TODO: fix
                        0L, // TODO: fix
                        DateTime.now(),
                        tupleValue.getString(2),
                        tupleValue.getBytes(4),
                        tupleValue.getBytes(3)
                    );
                    messages[i] = message;
                }

                return new ReadAllPage(
                    fromPositionInclusive,
                    0L, // TODO: fix. If we plan to use Versionstamp we can provide nextPosition unless we do something like nextPosition is populated if not at end and null if at end
                    maxCount >= kvs.size(),
                    direction,
                    readNext,
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
        Preconditions.checkNotNull(streamId);
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        HashCode streamHash = createHash(streamId);
        return database.read(tr -> {

            Subspace streamSubspace = getStreamSubspace(streamHash.toString());

            // add one so we can determine if we are at the end of the stream
            int rangeCount = maxCount + 1;

            // TODO: look at the various streaming modes to determine best fit.
            AsyncIterable<KeyValue> r = tr.getRange(streamSubspace.range(), rangeCount, reverse, StreamingMode.WANT_ALL);

            try {
                ReadDirection direction = reverse ? ReadDirection.BACKWARD : ReadDirection.FORWARD;

                ReadNextStreamPage readNext = (int nextPosition) -> readStreamForwards(streamId, fromVersionInclusive, maxCount);

                List<KeyValue> kvs = r.asList().get();
                if (kvs.isEmpty()) {
                    return new ReadStreamPage(
                        streamId,
                        PageReadStatus.STREAM_NOT_FOUND,
                        fromVersionInclusive,
                        StreamVersion.END,
                        StreamVersion.END,
                        StreamPosition.END,
                        direction,
                        true,
                        readNext,
                        Empty.STREAM_MESSAGES);
                }

                int limit = Math.min(maxCount, kvs.size());
                StreamMessage[] messages = new StreamMessage[limit];
                for (int i = 0; i < limit; i++) {
                    Tuple key = streamSubspace.unpack(kvs.get(i).getKey());
                    Tuple tupleValue = Tuple.fromBytes(kvs.get(i).getValue());
                    StreamMessage message = new StreamMessage(
                        streamId,
                        tupleValue.getUUID(0),
                        (int)key.getLong(0), // TODO: fix this.
                        0L, // TODO: fix
                        DateTime.now(),
                        tupleValue.getString(2),
                        tupleValue.getBytes(4),
                        tupleValue.getBytes(3)
                    );
                    messages[i] = message;
                }

                int nextStreamVersion = reverse
                    ? messages[limit - 1].getStreamVersion() - 1
                    : messages[limit - 1].getStreamVersion() + 1;

                return new ReadStreamPage(
                    streamId,
                    PageReadStatus.SUCCESS,
                    fromVersionInclusive,
                    nextStreamVersion,
                    0, // TODO: fix
                    0L, // TODO: fix
                    direction,
                    maxCount >= kvs.size(),
                    readNext,
                    messages);
            } catch (InterruptedException|ExecutionException e) {
                // TODO: what do we actually want to do here
                LOG.error("error reading from stream {}", streamId, e);
            }

            return null;
        });
    }

    @Override
    public Long readHeadPosition() {
        return database.read(tr -> {
            try {
                Subspace globalSubspace = getGlobalSubspace();
                byte[] k = tr.getKey(KeySelector.lastLessThan(globalSubspace.range().end)).get();

                if (ByteBuffer.wrap(k).compareTo(ByteBuffer.wrap(globalSubspace.range().begin)) < 0) {
                    return 0L;
                }

                Tuple t = globalSubspace.unpack(k);
                if (t == null) {
                    // TODO: custom exception
                    throw new RuntimeException("failed to unpack key");
                }

                return t.getLong(0);
            } catch (InterruptedException|ExecutionException e) {
                // TODO: what do we actually want to do here
                LOG.error("error reading head position", e);
            }

            return null;
        });
    }

    @Override
    public StreamMetadataResult getStreamMetadata(String streamId) {
        return null;
    }

    private static HashCode createHash(String streamId) {
        return Hashing.murmur3_128().hashString(streamId, UTF_8);
    }

    private Subspace getGlobalSubspace() {
        return esSubspace.subspace(Tuple.from(EventStoreSubspaces.GLOBAL.getValue()));
    }

    private Subspace getStreamSubspace(String streamHash) {
        return esSubspace.subspace(Tuple.from(EventStoreSubspaces.STREAM.getValue(), streamHash));
    }


//    https://forums.foundationdb.org/t/get-current-versionstamp/586/3
//    public CompletableFuture<Versionstamp> getCurVersionStamp(ReadTransaction tr) {
//        AsyncIterator<KeyValue> iterator = tr.getRange(logSubspace.range(), /* reverse = */ true, /* limit = */ 1).iterator();
//        return iterator.onHasNext(hasAny -> {
//            if (hasAny) {
//                // Get the last element from the log subspace and parse out the versionstamp
//                KeyValue kv = iterator.next();
//                return Tuple.fromBytes(kv.getKey()).getVersionstamp(0);
//            } else {
//                // Log subspace is empty
//                return null; // or a versionstamp of all zeroes if you prefer
//            }
//        });
//    }

//    Construct a versionstamp from your transaction's read version
//    This makes use of the fact that the first 8 bytes of a versionstamp are the commit version of the data associated with a record.
//    Therefore, if you know the read version of the transaction, you also know that all data in your log subspace at version v will be prefixed by a version less than or equal to v and all future data added later will be prefixed with a version greater than v.
//    So you can do something like:

//    public CompletableFuture<Versionstamp> getCurVersionStamp(ReadTransaction tr) {
//        return tr.getReadVersion().thenApply(readVersion ->
//            Versionstamp.fromBytes(ByteBuffer.allocate(Versionstamp.LENGTH)
//                .order(ByteOrder.BIG_ENDIAN)
//                .putLong(readVersion)
//                .putInt(0xffffffff)
//                .array())
//        );
//    }
}
