package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class EventStoreLayer implements EventStore {

    private static final Logger LOG = LoggerFactory.getLogger(EventStoreLayer.class);

    public static final int MAX_READ_SIZE = 4096;

    private final Database database;
    private final DirectorySubspace esSubspace;

    /**
     *
     * @param database the foundationDB database
     * @param subspace the directory you wish use to store events for your event store. TODO: does this need to allow manual subspaces?
     */
    public EventStoreLayer(Database database, DirectorySubspace subspace) {
        this.database = database;
        this.esSubspace = subspace;
    }

    /**
     * Default factory method
     * @param database the foundationDB database
     * @return an EventStoreLayer with a under an "es" Directory
     * @throws ExecutionException
     * @throws InterruptedException
     * @see EventStoreLayer#getDefaultDirectorySubspace
     */
    public static EventStoreLayer getDefault(Database database) throws ExecutionException, InterruptedException {
        DirectorySubspace esSubspace = getDefaultDirectorySubspace(database);
        return new EventStoreLayer(database, esSubspace);
    }

    /**
     *
     * @param database
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static DirectorySubspace getDefaultDirectorySubspace(Database database) throws ExecutionException, InterruptedException {
        return DirectoryLayer.getDefault().createOrOpen(database, Collections.singletonList("es")).get();
    }

    @Override
    public AppendResult appendToStream(String streamId, long expectedVersion, NewStreamMessage... messages) throws InterruptedException, ExecutionException {
        Preconditions.checkNotNull(streamId);
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        StreamId stream = new StreamId(streamId);

        if (expectedVersion == ExpectedVersion.ANY) {
            return appendToStreamExpectedVersionAny(stream, messages);
        }
        if (expectedVersion == ExpectedVersion.NO_STREAM) {
            return appendToStreamExpectedVersionNoStream(stream, messages);
        }
        return appendToStreamExpectedVersion(stream, expectedVersion, messages);
    }

    // TODO: clean up
    private AppendResult appendToStreamExpectedVersionAny(StreamId streamId, NewStreamMessage[] messages) throws ExecutionException, InterruptedException {
        ReadEventResult readEventResult = readEventInternal(streamId, StreamPosition.END);
        // TODO: do we need to do any version/event number checking?

        Subspace globalSubspace = getGlobalSubspace();
        Subspace streamSubspace = getStreamSubspace(streamId);

        // TODO: check
        AtomicLong latestStreamVersion = new AtomicLong(readEventResult.getEventNumber());
        CompletableFuture<byte[]> trVersionFuture = database.run(tr -> {
            for (int i = 0; i < messages.length; i++) {
                // TODO: not a huge fan of "Version" or "StreamVersion" nomenclature/language especially when
                // eventstore bounces between those as well as position and event number
                long eventNumber = latestStreamVersion.incrementAndGet();

                Versionstamp versionstamp = Versionstamp.incomplete(i);

                NewStreamMessage message = messages[i];

                // TODO: should this be outside the loop? does it matter?
                long createdDateUtcEpoch = Instant.now().toEpochMilli();
                Tuple globalSubspaceValue = Tuple.from(message.getMessageId(), streamId.getOriginalId(), message.getType(), message.getData(), message.getMetadata(), eventNumber, createdDateUtcEpoch);
                Tuple streamSubspaceValue = Tuple.from(message.getMessageId(), streamId.getOriginalId(), message.getType(), message.getData(), message.getMetadata(), eventNumber, createdDateUtcEpoch, versionstamp);
                // TODO: probably shouldn't store the event twice. Instead maybe we should just store a pointer
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), globalSubspaceValue.pack());
                tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(eventNumber)).pack(), streamSubspaceValue.packWithVersionstamp());
            }

            return tr.getVersionstamp();
        });

        byte[] trVersion = trVersionFuture.get();
        Versionstamp completedVersion = Versionstamp.complete(trVersion, messages.length - 1);
        return new AppendResult(latestStreamVersion.get(), completedVersion);
    }

    // TODO: clean up
    private AppendResult appendToStreamExpectedVersionNoStream(StreamId streamId, NewStreamMessage[] messages) throws ExecutionException, InterruptedException {
        ReadStreamPage backwardPage = readStreamBackwardsInternal(streamId, StreamPosition.END, 1);
        if (PageReadStatus.STREAM_NOT_FOUND != backwardPage.getStatus()) {
            // ErrorMessages.AppendFailedWrongExpectedVersion
            // $"Append failed due to WrongExpectedVersion.Stream: {streamId}, Expected version: {expectedVersion}"
            throw new WrongExpectedVersionException(String.format("Append failed due to wrong expected version. Stream %s. Expected version: %d.", streamId.getOriginalId(), StreamVersion.NONE));
        }

        Subspace globalSubspace = getGlobalSubspace();
        Subspace streamSubspace = getStreamSubspace(streamId);

        CompletableFuture<byte[]> trVersionFuture = database.run(tr -> {
            for (int i = 0; i < messages.length; i++) {
                Versionstamp versionstamp = Versionstamp.incomplete(i);

                NewStreamMessage message = messages[i];
                long createdDateUtcEpoch = Instant.now().toEpochMilli();
                Tuple globalSubspaceValue = Tuple.from(message.getMessageId(), streamId.getOriginalId(), message.getType(), message.getData(), message.getMetadata(), i, createdDateUtcEpoch);
                Tuple streamSubspaceValue = Tuple.from(message.getMessageId(), streamId.getOriginalId(), message.getType(), message.getData(), message.getMetadata(), i, createdDateUtcEpoch, versionstamp);
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), globalSubspaceValue.pack());
                tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(i)).pack(), streamSubspaceValue.packWithVersionstamp());
            }

            return tr.getVersionstamp();
        });

        byte[] trVersion = trVersionFuture.get();
        Versionstamp completedVersion = Versionstamp.complete(trVersion, messages.length - 1);
        return new AppendResult(messages.length - 1, completedVersion);
    }

    // TODO: clean up
    private AppendResult appendToStreamExpectedVersion(StreamId streamId, long expectedVersion, NewStreamMessage[] messages) throws ExecutionException, InterruptedException {
        ReadEventResult readEventResult = readEventInternal(streamId, StreamPosition.END);
        // TODO: do we need to do any version/event number checking?

        if (!Objects.equals(expectedVersion, readEventResult.getEventNumber())) {
            throw new WrongExpectedVersionException(String.format("Append failed due to wrong expected version. Stream %s. Expected version: %d. Current version %d.", streamId.getOriginalId(), expectedVersion, readEventResult.getEventNumber()));
        }

        Subspace globalSubspace = getGlobalSubspace();
        Subspace streamSubspace = getStreamSubspace(streamId);

        AtomicLong latestStreamVersion = new AtomicLong(readEventResult.getEventNumber());
        CompletableFuture<byte[]> trVersionFuture = database.run(tr -> {

            for (int i = 0; i < messages.length; i++) {
                long eventNumber = latestStreamVersion.incrementAndGet();

                Versionstamp versionstamp = Versionstamp.incomplete(i);

                long createdDateUtcEpoch = Instant.now().toEpochMilli();
                NewStreamMessage message = messages[i];
                Tuple globalSubspaceValue = Tuple.from(message.getMessageId(), streamId.getOriginalId(), message.getType(), message.getData(), message.getMetadata(), eventNumber, createdDateUtcEpoch);
                Tuple streamSubspaceValue = Tuple.from(message.getMessageId(), streamId.getOriginalId(), message.getType(), message.getData(), message.getMetadata(), eventNumber, createdDateUtcEpoch, versionstamp);
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
        return readAllForwardInternal(fromPositionInclusive, maxCount, false);
    }

    @Override
    public ReadAllPage readAllBackwards(Versionstamp fromPositionInclusive, int maxCount) throws InterruptedException, ExecutionException {
        return readAllBackwardInternal(fromPositionInclusive, maxCount, true);
    }

    private ReadAllPage readAllForwardInternal(Versionstamp fromPositionInclusive, int maxCount, boolean reverse) throws ExecutionException, InterruptedException {
        Preconditions.checkNotNull(fromPositionInclusive);
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        Subspace globalSubspace = getGlobalSubspace();

        List<KeyValue> kvs = database.read(tr -> {
            // add one so we can determine if we are at the end of the stream
            int rangeCount = maxCount + 1;

            KeySelector begin = Objects.equals(fromPositionInclusive, Position.END)
                ? KeySelector.lastLessOrEqual(globalSubspace.range().end)
                : KeySelector.firstGreaterOrEqual(globalSubspace.pack(fromPositionInclusive));

            return tr.getRange(
                begin,
                KeySelector.firstGreaterOrEqual(globalSubspace.range().end),
                rangeCount,
                false,
                StreamingMode.WANT_ALL).asList();
        }).get();

        ReadDirection direction = reverse ? ReadDirection.BACKWARD : ReadDirection.FORWARD;
        ReadNextAllPage readNext = (Versionstamp nextPosition) -> readAllForwards(nextPosition, maxCount);

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
        // TODO: Review / fix this.
        Versionstamp nextPosition = maxCount >= kvs.size()
            ? null
            : globalSubspace.unpack(kvs.get(maxCount).getKey()).getVersionstamp(0);

        return new ReadAllPage(
            fromPositionInclusive,
            nextPosition,
            maxCount >= kvs.size(),
            direction,
            readNext,
            messages);
    }

    private ReadAllPage readAllBackwardInternal(Versionstamp fromPositionInclusive, int maxCount, boolean reverse) throws ExecutionException, InterruptedException {
        Preconditions.checkNotNull(fromPositionInclusive);
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        Subspace globalSubspace = getGlobalSubspace();

        List<KeyValue> kvs = database.read(tr -> {
            // add one so we can determine if we are at the end of the stream
            int rangeCount = maxCount + 1;

            final KeySelector end;
            if (Objects.equals(fromPositionInclusive, Position.START)) {
                // firstGreaterThan (+1) doesn't work when attempting to get start position
                // Seems like range queries don't work when begin has firstGreaterOrEqual and end with firstGreaterThan or firstGreaterOrEqual
                // so will bump the offset by 2
                end = new KeySelector(globalSubspace.range().begin, false, 2);
            } else if (Objects.equals(fromPositionInclusive, Position.END)) {
                end = KeySelector.firstGreaterThan(globalSubspace.range().end);
            } else {
                end = KeySelector.firstGreaterThan(globalSubspace.pack(Tuple.from(fromPositionInclusive)));
            }

            return tr.getRange(
                KeySelector.firstGreaterOrEqual(globalSubspace.range().begin),
                end,
                rangeCount,
                true,
                StreamingMode.WANT_ALL).asList();
        }).get();

        ReadDirection direction = reverse ? ReadDirection.BACKWARD : ReadDirection.FORWARD;
        ReadNextAllPage readNext = (Versionstamp nextPosition) -> readAllForwards(nextPosition, maxCount);

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
        // TODO: Review / fix this.
        Versionstamp nextPosition = maxCount >= kvs.size()
            ? null
            : globalSubspace.unpack(kvs.get(maxCount).getKey()).getVersionstamp(0);

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
        return readStreamForwardsInternal(new StreamId(streamId), fromVersionInclusive, maxCount);
    }

    private ReadStreamPage readStreamForwardsInternal(StreamId streamId, long fromVersionInclusive, int maxCount) throws ExecutionException, InterruptedException {
        Preconditions.checkNotNull(streamId);
        Preconditions.checkArgument(fromVersionInclusive >= -1, "fromVersionInclusive must greater than -1");
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        Subspace streamSubspace = getStreamSubspace(streamId);

        List<KeyValue> kvs = database.read(tr -> {
            // add one so we can determine if we are at the end of the stream
            int rangeCount = maxCount + 1;

            KeySelector begin = fromVersionInclusive == StreamPosition.END
                ? KeySelector.lastLessOrEqual(streamSubspace.range().end)
                : KeySelector.firstGreaterOrEqual(streamSubspace.pack(fromVersionInclusive));

            return tr.getRange(
                begin,
                KeySelector.firstGreaterOrEqual(streamSubspace.range().end),
                rangeCount,
                false,
                StreamingMode.WANT_ALL).asList();
        }).get();

        ReadNextStreamPage readNext = (long nextPosition) -> readStreamForwardsInternal(streamId, nextPosition, maxCount);

        if (kvs.isEmpty()) {
            return new ReadStreamPage(
                streamId.getOriginalId(),
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
            Tuple tupleValue = Tuple.fromBytes(kv.getValue());
            StreamMessage message = new StreamMessage(
                streamId.getOriginalId(),
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

        // TODO: review this. What should next position be if at end and when not at end?
        Tuple nextPositionValue = Tuple.fromBytes(kvs.get(limit - 1).getValue());
        long nextPosition = nextPositionValue.getLong(5) + 1;

        return new ReadStreamPage(
            streamId.getOriginalId(),
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
        return readStreamBackwardsInternal(new StreamId(streamId), fromVersionInclusive, maxCount);
    }

    private ReadStreamPage readStreamBackwardsInternal(StreamId streamId, long fromVersionInclusive, int maxCount) throws ExecutionException, InterruptedException {
        Preconditions.checkNotNull(streamId);
        Preconditions.checkArgument(fromVersionInclusive >= -1, "fromVersionInclusive must greater than -1");
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        Subspace streamSubspace = getStreamSubspace(streamId);

        List<KeyValue> kvs = database.read(tr -> {
            // add one so we can determine if we are at the end of the stream
            int rangeCount = maxCount + 1;
            return tr.getRange(
                streamSubspace.pack(Tuple.from(fromVersionInclusive - maxCount)),
                // adding one because readTransaction.getRange's end range is exclusive
                streamSubspace.pack(Tuple.from(fromVersionInclusive == StreamPosition.END ? Long.MAX_VALUE : fromVersionInclusive + 1)),
                rangeCount,
                true,
                StreamingMode.WANT_ALL).asList();
        }).get();

        ReadNextStreamPage readNext = (long nextPosition) -> readStreamBackwardsInternal(streamId, nextPosition, maxCount);

        if (kvs.isEmpty()) {
            return new ReadStreamPage(
                streamId.getOriginalId(),
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
            Tuple tupleValue = Tuple.fromBytes(kv.getValue());
            StreamMessage message = new StreamMessage(
                streamId.getOriginalId(),
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

        // TODO: review this. What should next position be if at end and when not at end?
        Tuple lastTuple = Tuple.fromBytes(kvs.get(limit - 1).getValue());
        long nextPosition = lastTuple.getLong(5) - 1;

        return new ReadStreamPage(
            streamId.getOriginalId(),
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
    public Versionstamp readHeadPosition() throws ExecutionException, InterruptedException {
        Subspace globalSubspace = getGlobalSubspace();

        byte[] k = database.read(tr -> tr.getKey(KeySelector.lastLessThan(globalSubspace.range().end))).get();

        if (ByteBuffer.wrap(k).compareTo(ByteBuffer.wrap(globalSubspace.range().begin)) < 0) {
            return null;
        }

        Tuple t = globalSubspace.unpack(k);
        // TODO: can this ever be null?
        if (t == null) {
            // TODO: custom exception
            throw new RuntimeException("failed to unpack key");
        }

        return t.getVersionstamp(0);
    }

    @Override
    public StreamMetadataResult getStreamMetadata(String streamId) {
        return null;
    }

    @Override
    public ReadEventResult readEvent(String stream, long eventNumber) throws ExecutionException, InterruptedException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(stream));
        Preconditions.checkArgument(eventNumber >= -1);

        return readEventInternal(new StreamId(stream), eventNumber);
    }

    private ReadEventResult readEventInternal(StreamId streamId, long eventNumber) throws ExecutionException, InterruptedException {
        Subspace streamSubspace = getStreamSubspace(streamId);

        if (Objects.equals(eventNumber, StreamPosition.END)) {
            ReadStreamPage read = readStreamBackwardsInternal(streamId, StreamPosition.END, 1);
            if (read.getStatus() == PageReadStatus.STREAM_NOT_FOUND) {
                return new ReadEventResult(ReadEventStatus.NOT_FOUND, streamId.getOriginalId(), eventNumber, null);
            }

            return new ReadEventResult(ReadEventStatus.SUCCESS, streamId.getOriginalId(), read.getMessages()[0].getStreamVersion(), read.getMessages()[0]);
        } else {
            byte[] valueBytes = database.read(tr -> tr.get(streamSubspace.pack(Tuple.from(eventNumber)))).get();
            if (valueBytes == null) {
                return new ReadEventResult(ReadEventStatus.NOT_FOUND, streamId.getOriginalId(), eventNumber, null);
            }

            Tuple value = Tuple.fromBytes(valueBytes);
            StreamMessage message = new StreamMessage(
                streamId.getOriginalId(),
                value.getUUID(0),
                value.getLong(5),
                value.getVersionstamp(7),
                value.getLong(6),
                value.getString(2),
                value.getBytes(4),
                value.getBytes(3)
            );
            return new ReadEventResult(ReadEventStatus.SUCCESS, streamId.getOriginalId(), eventNumber, message);
        }
    }

    private Subspace getGlobalSubspace() {
        return esSubspace.subspace(Tuple.from(EventStoreSubspaces.GLOBAL.getValue()));
    }

    private Subspace getStreamSubspace(StreamId streamId) {
        return esSubspace.subspace(Tuple.from(EventStoreSubspaces.STREAM.getValue(), streamId.getHash()));
    }

}
