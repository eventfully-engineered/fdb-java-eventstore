package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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
                    // TODO: not a huge fan of "Version" or "StreamVersion" nomenclature/language especially when
                    // eventstore bounces between those as well as position and event number
                    int eventNumber = latestStreamVersion.incrementAndGet();

                    Versionstamp versionstamp = Versionstamp.incomplete(i);

                    // TODO: how should we store metadata
                    NewStreamMessage message = messages[i];
                    Tuple tv = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getData(), message.getMetadata(), eventNumber, DateTime.now(DateTimeZone.UTC).getMillis());

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
        // TODO: set currentPosition
        try {
            byte[] trVersion = trVersionFuture.get();

            Versionstamp completedVersion = Versionstamp.complete(trVersion, messages.length - 1);
            return new AppendResult(latestStreamVersion.get(), completedVersion);

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

                // TODO: is StreamVersion.END right?
                latestStreamVersion.set(StreamVersion.END);
                for (int i = 0; i < messages.length; i++) {
                    int eventNumber = latestStreamVersion.incrementAndGet();

                    Versionstamp versionstamp = Versionstamp.incomplete(i);

                    // TODO: how should we store metadata
                    NewStreamMessage message = messages[i];
                    Tuple tv = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getData(), message.getMetadata(), eventNumber, DateTime.now(DateTimeZone.UTC).getMillis());

                    tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), tv.pack());
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(latestStreamVersion)).pack(), tv.add(versionstamp).packWithVersionstamp());
                }

                return tr.getVersionstamp();
            } catch (Exception e) {
                // TODO: is there a way to only catch conflicts?
                // TODO: what to actually do here
                LOG.error("error appending to stream", e);
            }

            return null;
        });

        // TODO: all this is pretty terrible
        // TODO: set currentPosition
        try {
            byte[] trVersion = trVersionFuture.get();

            Versionstamp completedVersion = Versionstamp.complete(trVersion, messages.length - 1);
            return new AppendResult(latestStreamVersion.get(), completedVersion);

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
                    int eventNumber = latestStreamVersion.incrementAndGet();

                    Versionstamp versionstamp = Versionstamp.incomplete(i);

                    // TODO: how should we store metadata
                    long createdDateUtcEpoch = DateTime.now(DateTimeZone.UTC).getMillis();
                    NewStreamMessage message = messages[i];
                    Tuple globalSubspaceValue = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getData(), message.getMetadata(), eventNumber, createdDateUtcEpoch);
                    Tuple streamSubspaceValue = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getData(), message.getMetadata(), eventNumber, createdDateUtcEpoch, versionstamp);
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), globalSubspaceValue.pack());
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(latestStreamVersion)).pack(), streamSubspaceValue.packWithVersionstamp());
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
            // TODO: set currentPosition
            Versionstamp completedVersion = Versionstamp.complete(trVersion, messages.length - 1);
            return new AppendResult(latestStreamVersion.get(), completedVersion);

        } catch (InterruptedException|ExecutionException e) {
            // TODO: what to actually do here
            LOG.error("error appending to stream", e);
        }

        return null;
    }

    @Override
    public void deleteStream(String streamId, int expectedVersion) {
        // TODO: how to handle?
        // We can clear the stream subspace via clear(Range) but how to delete from global subspace?
        // would we need a scavenger process? something else?
        // database.run(tr -> {);
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
    public ReadAllPage readAllForwards(Versionstamp fromPositionInclusive, int maxCount) {
        return readAllInternal(fromPositionInclusive, maxCount, false);
    }

    @Override
    public ReadAllPage readAllBackwards(Versionstamp fromPositionInclusive, int maxCount) {
        return readAllInternal(fromPositionInclusive, maxCount, true);
    }

    private ReadAllPage readAllInternal(Versionstamp fromPositionInclusive, int maxCount, boolean reverse) {
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        return database.read(tr -> {

            Subspace globalSubspace = getGlobalSubspace();

            // add one so we can determine if we are at the end of the stream
            int rangeCount =  maxCount + 1;

            // TODO: look at the various streaming modes to determine best fit
            // TODO: fix range query
            // TODO: would need to change based on forward or backwards
            // Ranges appear to be begin (inclusive) and end (exclusive) and not sure how we could have begin be exclusive
            // TODO: we may need to do something different for begin and end when reverse
            AsyncIterable<KeyValue> r = tr.getRange(
                globalSubspace.pack(Tuple.from(fromPositionInclusive)),
                globalSubspace.pack(Position.END),
                rangeCount,
                reverse,
                StreamingMode.WANT_ALL);
            try {
                ReadDirection direction = reverse ? ReadDirection.BACKWARD : ReadDirection.FORWARD;

                ReadNextAllPage readNext = (Versionstamp nextPosition) -> readAllForwards(nextPosition, maxCount);

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
                    KeyValue kv = kvs.get(i);
                    Tuple key = globalSubspace.unpack(kv.getKey());
                    Tuple tupleValue = Tuple.fromBytes(kv.getValue());

                    // TODO: how to handle streamId, messageId, stream version, position, etc...
                    StreamMessage message = new StreamMessage(
                        tupleValue.getString(1),
                        tupleValue.getUUID(0),
                        (int)tupleValue.getLong(5),
                        key.getVersionstamp(0),
                        tupleValue.getLong(6),
                        tupleValue.getString(2),
                        tupleValue.getBytes(4),
                        tupleValue.getBytes(3)
                    );
                    messages[i] = message;
                }

                // if we are at the end return next position as null otherwise
                // grab it from the last item from the range query which is outside the slice we want
                // TODO: fix this.
                final Versionstamp nextPosition;
                if (maxCount >= kvs.size()) {
                    nextPosition = null;
                } else {
                    Tuple nextPositionKey = globalSubspace.unpack(kvs.get(maxCount).getKey());
                    nextPosition = nextPositionKey.getVersionstamp(0);
                }

                return new ReadAllPage(
                    fromPositionInclusive,
                    nextPosition,
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
        Preconditions.checkArgument(fromVersionInclusive >= 0, "fromVersionInclusive must greater than 0");
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        HashCode streamHash = createHash(streamId);
        return database.read(tr -> {

            Subspace streamSubspace = getStreamSubspace(streamHash.toString());

            // add one so we can determine if we are at the end of the stream
            int rangeCount = maxCount + 1;

            // TODO: look at the various streaming modes to determine best fit.
            // TODO: fix range with paging
            // TODO: we may need to do something different for begin and end when reverse
            AsyncIterable<KeyValue> r = tr.getRange(
                streamSubspace.pack(Tuple.from(fromVersionInclusive)),
                streamSubspace.pack(Tuple.from(Integer.MAX_VALUE)),
                rangeCount,
                reverse,
                StreamingMode.WANT_ALL);

            try {
                ReadDirection direction = reverse ? ReadDirection.BACKWARD : ReadDirection.FORWARD;

                ReadNextStreamPage readNext = (int nextPosition) -> readStreamForwards(streamId, nextPosition, maxCount);

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
                    KeyValue kv = kvs.get(i);
                    Tuple key = streamSubspace.unpack(kv.getKey());
                    Tuple tupleValue = Tuple.fromBytes(kv.getValue());
                    StreamMessage message = new StreamMessage(
                        streamId,
                        tupleValue.getUUID(0),
                        (int)tupleValue.getLong(5),
                        tupleValue.getVersionstamp(7),
                        tupleValue.getLong(6),
                        tupleValue.getString(2),
                        tupleValue.getBytes(4),
                        tupleValue.getBytes(3)
                    );
                    messages[i] = message;
                }

                final int nextPosition;
                if (maxCount >= kvs.size()) {
                    nextPosition = StreamPosition.END;
                } else {
                    Tuple nextPositionValue = Tuple.fromBytes(kvs.get(maxCount).getValue());
                    nextPosition = (int)nextPositionValue.getLong(5);
                }

                return new ReadStreamPage(
                    streamId,
                    PageReadStatus.SUCCESS,
                    fromVersionInclusive,
                    nextPosition,
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

    @Override
    public ReadEventResult readEvent(String stream, long eventNumber) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(stream));
        Preconditions.checkArgument(eventNumber >= -1);

        HashCode streamHash = createHash(stream);
        return database.read(tr -> {

            Subspace streamSubspace = getStreamSubspace(streamHash.toString());

            try {

                byte[] bytes = tr.get(streamSubspace.pack(Tuple.from(eventNumber))).get();
                if (bytes == null) {
                    return new ReadEventResult(ReadEventStatus.NO_STREAM, stream, eventNumber, null);
                }

                Tuple value = Tuple.fromBytes(bytes);
                StreamMessage message = new StreamMessage(
                    stream,
                    value.getUUID(0),
                    (int)value.getLong(5),
                    value.getVersionstamp(7),
                    value.getLong(6),
                    value.getString(2),
                    value.getBytes(4),
                    value.getBytes(3)
                );
                return new ReadEventResult(ReadEventStatus.SUCCESS, stream, eventNumber, message);

            } catch (InterruptedException|ExecutionException e) {
                // TODO: what do we actually want to do here
                LOG.error("error reading head position", e);
            }

            return null;
        });
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
