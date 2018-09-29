package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;


// TODO: use Long.parseUnsignedLong
// TODO: should be closeable?
// TODO: where should we store stream metadata?
// TODO: whats a common pattern for FoundationDB layers?
// - Should our operations create their own transaction? If so how can clients make sure everything is one atomic transaction?
// - Should you have clients pass in a transaction?
// - Should clients pass in their on directory/subspace?
// - How do we want to handle position in global vs stream subspace?
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
    public AppendResult appendToStream(String streamId, long expectedVersion, NewStreamMessage... messages) throws InterruptedException, ExecutionException {
        return appendToStreamInternal(streamId, expectedVersion, messages);
    }

    private AppendResult appendToStreamInternal(String streamId, long expectedVersion, NewStreamMessage[] messages) throws InterruptedException, ExecutionException {
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
    private AppendResult appendToStreamExpectedVersionAny(String streamId, NewStreamMessage[] messages) throws ExecutionException, InterruptedException {
        // TODO: hashing multiple times...once here and once in readStreamBackwards
        HashCode streamHash = createHash(streamId);

        ReadStreamPage backwardPage = readStreamBackwards(streamId, 0, 1);
        long currentStreamVersion = backwardPage.getMessages().length == 0
            ? StreamVersion.END
            : backwardPage.getNextStreamVersion();

        Subspace globalSubspace = getGlobalSubspace();
        Subspace streamSubspace = getStreamSubspace(streamHash.toString());

        AtomicLong latestStreamVersion = new AtomicLong(currentStreamVersion);
        CompletableFuture<byte[]> trVersionFuture = database.run(tr -> {
            for (int i = 0; i < messages.length; i++) {
                // TODO: not a huge fan of "Version" or "StreamVersion" nomenclature/language especially when
                // eventstore bounces between those as well as position and event number
                long eventNumber = latestStreamVersion.incrementAndGet();

                Versionstamp versionstamp = Versionstamp.incomplete(i);

                // TODO: how should we store metadata
                NewStreamMessage message = messages[i];
                Tuple tv = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getData(), message.getMetadata(), eventNumber, Instant.now().toEpochMilli());

                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), tv.pack());
                tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(latestStreamVersion)).pack(), tv.add(versionstamp).packWithVersionstamp());
            }

            return tr.getVersionstamp();
        });

        byte[] trVersion = trVersionFuture.get();
        Versionstamp completedVersion = Versionstamp.complete(trVersion, messages.length - 1);
        return new AppendResult(latestStreamVersion.get(), completedVersion);
    }

    // TODO: Idempotency handling. Check if the Messages have already been written.
    // TODO: clean up
    private AppendResult appendToStreamExpectedVersionNoStream(String streamId, NewStreamMessage[] messages) throws ExecutionException, InterruptedException {
        // TODO: hashing multiple times...once here and once in readStreamBackwards
        HashCode streamHash = createHash(streamId);

        ReadStreamPage backwardPage = readStreamBackwards(streamId, 0, 1);

        if (PageReadStatus.STREAM_NOT_FOUND != backwardPage.getStatus()) {
            // ErrorMessages.AppendFailedWrongExpectedVersion
            // $"Append failed due to WrongExpectedVersion.Stream: {streamId}, Expected version: {expectedVersion}"
            throw new WrongExpectedVersionException(String.format("Append failed due to wrong expected version. Stream %s. Expected version: %d.", streamId, StreamVersion.NONE));
        }

        Subspace globalSubspace = getGlobalSubspace();
        Subspace streamSubspace = getStreamSubspace(streamHash.toString());

        // TODO: is StreamVersion.END right?
        AtomicLong latestStreamVersion = new AtomicLong(StreamVersion.END);
        CompletableFuture<byte[]> trVersionFuture = database.run(tr -> {
            for (int i = 0; i < messages.length; i++) {
                long eventNumber = latestStreamVersion.incrementAndGet();

                Versionstamp versionstamp = Versionstamp.incomplete(i);

                // TODO: how should we store metadata
                NewStreamMessage message = messages[i];
                Tuple tv = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getData(), message.getMetadata(), eventNumber, Instant.now().toEpochMilli());

                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), tv.pack());
                tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(latestStreamVersion)).pack(), tv.add(versionstamp).packWithVersionstamp());
            }

            return tr.getVersionstamp();
        });

        byte[] trVersion = trVersionFuture.get();
        Versionstamp completedVersion = Versionstamp.complete(trVersion, messages.length - 1);
        return new AppendResult(latestStreamVersion.get(), completedVersion);
    }

    // TODO: Idempotency handling. Check if the Messages have already been written.
    // TODO: clean up
    private AppendResult appendToStreamExpectedVersion(String streamId, long expectedVersion, NewStreamMessage[] messages) throws ExecutionException, InterruptedException {
        // TODO: hashing multiple times...once here and once in readStreamBackwards
        HashCode streamHash = createHash(streamId);

        ReadStreamPage backwardPage = readStreamBackwards(streamId, 0, 1);

        long currentStreamVersion = backwardPage.getMessages().length == 0
            ? StreamVersion.END
            : backwardPage.getNextStreamVersion(); // TODO: this is wrong

        if (!Objects.equals(expectedVersion, currentStreamVersion)) {
            throw new WrongExpectedVersionException(String.format("Append failed due to wrong expected version. Stream %s. Expected version: %d. Current version %d.", streamId, expectedVersion, currentStreamVersion));
        }

        Subspace globalSubspace = getGlobalSubspace();
        Subspace streamSubspace = getStreamSubspace(streamHash.toString());

        AtomicLong latestStreamVersion = new AtomicLong(currentStreamVersion);
        CompletableFuture<byte[]> trVersionFuture = database.run(tr -> {

            for (int i = 0; i < messages.length; i++) {
                long eventNumber = latestStreamVersion.incrementAndGet();

                Versionstamp versionstamp = Versionstamp.incomplete(i);

                // TODO: how should we store metadata
                long createdDateUtcEpoch = Instant.now().toEpochMilli();
                NewStreamMessage message = messages[i];
                Tuple globalSubspaceValue = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getData(), message.getMetadata(), eventNumber, createdDateUtcEpoch);
                Tuple streamSubspaceValue = Tuple.from(message.getMessageId(), streamId, message.getType(), message.getData(), message.getMetadata(), eventNumber, createdDateUtcEpoch, versionstamp);
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), globalSubspaceValue.pack());
                tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(latestStreamVersion)).pack(), streamSubspaceValue.packWithVersionstamp());
            }

            return tr.getVersionstamp();
        });

        byte[] trVersion = trVersionFuture.get();
        Versionstamp completedVersion = Versionstamp.complete(trVersion, messages.length - 1);
        return new AppendResult(latestStreamVersion.get(), completedVersion);
    }

    @Override
    public void deleteStream(String streamId, long expectedVersion) {
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
    public SetStreamMetadataResult setStreamMetadata(String streamId, long expectedStreamMetadataVersion, Integer maxAge, Integer maxCount, String metadataJson) {
        return null;
    }

    @Override
    public ReadAllPage readAllForwards(Versionstamp fromPositionInclusive, int maxCount) throws InterruptedException, ExecutionException {
        return readAllInternal(fromPositionInclusive, maxCount, false);
    }

    @Override
    public ReadAllPage readAllBackwards(Versionstamp fromPositionInclusive, int maxCount) throws InterruptedException, ExecutionException {
        return readAllInternal(fromPositionInclusive, maxCount, true);
    }

    private ReadAllPage readAllInternal(Versionstamp fromPositionInclusive, int maxCount, boolean reverse) throws ExecutionException, InterruptedException {
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        Subspace globalSubspace = getGlobalSubspace();

        CompletableFuture<List<KeyValue>> r = database.read(tr -> {
            // add one so we can determine if we are at the end of the stream
            int rangeCount = maxCount + 1;
            return tr.getRange(
                globalSubspace.pack(Tuple.from(fromPositionInclusive)),
                globalSubspace.pack(Position.END),
                rangeCount,
                reverse,
                StreamingMode.WANT_ALL).asList();
        });

        ReadDirection direction = reverse ? ReadDirection.BACKWARD : ReadDirection.FORWARD;
        ReadNextAllPage readNext = (Versionstamp nextPosition) -> readAllForwards(nextPosition, maxCount);

        List<KeyValue> kvs = r.get();
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

            StreamMessage message = new StreamMessage(
                tupleValue.getString(1),
                tupleValue.getUUID(0),
                tupleValue.getLong(5),
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
    }

    @Override
    public ReadStreamPage readStreamForwards(String streamId, long fromVersionInclusive, int maxCount) throws ExecutionException, InterruptedException {
        Preconditions.checkNotNull(streamId);
        Preconditions.checkArgument(fromVersionInclusive >= -1, "fromVersionInclusive must greater than -1");
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        HashCode streamHash = createHash(streamId);

        Subspace streamSubspace = getStreamSubspace(streamHash.toString());

        CompletableFuture<List<KeyValue>> r = database.read(tr -> {
            // add one so we can determine if we are at the end of the stream
            int rangeCount = maxCount + 1;
            return tr.getRange(
                streamSubspace.pack(Tuple.from(fromVersionInclusive)),
                streamSubspace.pack(Tuple.from(Long.MAX_VALUE)),
                rangeCount,
                false,
                StreamingMode.WANT_ALL).asList();
        });

        ReadNextStreamPage readNext = (long nextPosition) -> readStreamForwards(streamId, nextPosition, maxCount);

        List<KeyValue> kvs = r.get();
        if (kvs.isEmpty()) {
            return new ReadStreamPage(
                streamId,
                PageReadStatus.STREAM_NOT_FOUND,
                fromVersionInclusive,
                StreamVersion.END,
                StreamVersion.END,
                StreamPosition.END,
                ReadDirection.FORWARD,
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
                tupleValue.getLong(5),
                tupleValue.getVersionstamp(7),
                tupleValue.getLong(6),
                tupleValue.getString(2),
                tupleValue.getBytes(4),
                tupleValue.getBytes(3)
            );
            messages[i] = message;
        }

        final long nextPosition;
        if (maxCount >= kvs.size()) {
            nextPosition = StreamPosition.END;
        } else {
            Tuple nextPositionValue = Tuple.fromBytes(kvs.get(maxCount).getValue());
            nextPosition = nextPositionValue.getLong(5);
        }

        return new ReadStreamPage(
            streamId,
            PageReadStatus.SUCCESS,
            fromVersionInclusive,
            nextPosition,
            0, // TODO: fix
            0L, // TODO: fix
            ReadDirection.FORWARD,
            maxCount >= kvs.size(),
            readNext,
            messages);
    }

    @Override
    public ReadStreamPage readStreamBackwards(String streamId, long fromVersionInclusive, int maxCount) throws ExecutionException, InterruptedException {
        Preconditions.checkNotNull(streamId);
        Preconditions.checkArgument(fromVersionInclusive >= -1, "fromVersionInclusive must greater than -1");
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        HashCode streamHash = createHash(streamId);

        Subspace streamSubspace = getStreamSubspace(streamHash.toString());

        CompletableFuture<List<KeyValue>> r = database.read(tr -> {
            // add one so we can determine if we are at the end of the stream
            int rangeCount = maxCount + 1;
            return tr.getRange(
                streamSubspace.pack(Tuple.from(fromVersionInclusive - maxCount)),
                // TODO: adding one because end range is exclusive
                streamSubspace.pack(Tuple.from(fromVersionInclusive == StreamPosition.END ? Long.MAX_VALUE : fromVersionInclusive + 1)),
                rangeCount,
                true,
                StreamingMode.WANT_ALL).asList();
        });

        ReadNextStreamPage readNext = (long nextPosition) -> readStreamForwards(streamId, nextPosition, maxCount);

        List<KeyValue> kvs = r.get();
        if (kvs.isEmpty()) {
            return new ReadStreamPage(
                streamId,
                PageReadStatus.STREAM_NOT_FOUND,
                fromVersionInclusive,
                StreamVersion.END,
                StreamVersion.END,
                StreamPosition.END,
                ReadDirection.BACKWARD,
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
                tupleValue.getLong(5),
                tupleValue.getVersionstamp(7),
                tupleValue.getLong(6),
                tupleValue.getString(2),
                tupleValue.getBytes(4),
                tupleValue.getBytes(3)
            );
            messages[i] = message;
        }

        final long nextPosition;
        if (maxCount >= kvs.size()) {
            nextPosition = StreamPosition.END;
        } else {
            Tuple nextPositionValue = Tuple.fromBytes(kvs.get(maxCount).getValue());
            nextPosition = nextPositionValue.getLong(5);
        }

        return new ReadStreamPage(
            streamId,
            PageReadStatus.SUCCESS,
            fromVersionInclusive,
            nextPosition,
            0, // TODO: fix
            0L, // TODO: fix
            ReadDirection.BACKWARD,
            maxCount >= kvs.size(),
            readNext,
            messages);
    }

    @Override
    public Long readHeadPosition() throws ExecutionException, InterruptedException {
        Subspace globalSubspace = getGlobalSubspace();

        byte[] k = database.read(tr -> tr.getKey(KeySelector.lastLessThan(globalSubspace.range().end))).get();

        if (ByteBuffer.wrap(k).compareTo(ByteBuffer.wrap(globalSubspace.range().begin)) < 0) {
            return 0L;
        }

        Tuple t = globalSubspace.unpack(k);
        // TODO: can this ever be null?
        if (t == null) {
            // TODO: custom exception
            throw new RuntimeException("failed to unpack key");
        }

        return t.getLong(0);
    }

    @Override
    public StreamMetadataResult getStreamMetadata(String streamId) {
        return null;
    }

    @Override
    public ReadEventResult readEvent(String stream, long eventNumber) throws ExecutionException, InterruptedException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(stream));
        Preconditions.checkArgument(eventNumber >= -1);

        HashCode streamHash = createHash(stream);
        Subspace streamSubspace = getStreamSubspace(streamHash.toString());

        byte[] valueBytes = database.read(tr -> {
                byte[] key = Objects.equals(eventNumber, StreamPosition.END)
                    ? tr.getKey(KeySelector.lastLessThan(streamSubspace.range().end)).join()
                    : streamSubspace.pack(Tuple.from(eventNumber));

                return tr.get(key);
        }).get();

        if (valueBytes == null) {
            return new ReadEventResult(ReadEventStatus.NOT_FOUND, stream, eventNumber, null);
        }

        Tuple value = Tuple.fromBytes(valueBytes);
        StreamMessage message = new StreamMessage(
            stream,
            value.getUUID(0),
            value.getLong(5),
            value.getVersionstamp(7),
            value.getLong(6),
            value.getString(2),
            value.getBytes(4),
            value.getBytes(3)
        );
        return new ReadEventResult(ReadEventStatus.SUCCESS, stream, eventNumber, message);

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
//        AsyncIterator<KeyValue> iterator = tr.getRange(esSubspace.range(), /* limit = */ 1, /* reverse = */ true).iterator();
//        return iterator.onHasNext().thenApply(hasAny -> {
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
