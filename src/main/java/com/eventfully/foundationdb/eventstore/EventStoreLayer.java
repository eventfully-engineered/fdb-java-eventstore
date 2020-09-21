package com.eventfully.foundationdb.eventstore;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static java.util.concurrent.CompletableFuture.completedFuture;

// TODO: for append, should we be starting a transaction that encompasses the read + write just to make sure nothing
// else can write to our expected version?
// internal methods would need to take transaction

// TODO: update global subspace format to be `Global / [versionstamp] / [stream] / [event]

// TODO: run vs runAsync
// https://forums.foundationdb.org/t/fdbdatabase-usage-from-java-api/593/2
// run synchronously commit your transaction
// he runAsync retry loop will only call commit after the returned future has completed (so you could do something like read a key from database, make some write based on your read, and then commit).

//To define ranges that extend from the beginning the database, you can use the empty string '':
//
//    for k, v in tr.get_range('', 'm'):
//    print(k, v)
//    Likewise, to define ranges that extend to the end of the database, you can use the key '\xFF':
//
//    for k, v in tr.get_range('m', '\xFF'):
//    print(k, v)

// TODO: for multi-tenant that is using the shared cluster we need to allow passing in a directory
// so that we can separate tenants

// TODO: move preconditions out of completablefuture block

/**
 *
 */
public class EventStoreLayer implements EventStore {

    private static final Logger LOG = LoggerFactory.getLogger(EventStoreLayer.class);

    public static final int MAX_READ_SIZE = 4096;
    private static final String POINTER_DELIMITER = "@";

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
     * @see EventStoreLayer#getDefaultDirectorySubspace
     */
    public static CompletableFuture<EventStoreLayer> getDefault(Database database) {
        return getDefaultDirectorySubspace(database)
            .thenCompose(esSubspace -> completedFuture(new EventStoreLayer(database, esSubspace)));
    }

    /**
     *
     * @param database
     * @return
     */
    public static CompletableFuture<DirectorySubspace> getDefaultDirectorySubspace(Database database) {
        return DirectoryLayer.getDefault().createOrOpen(database, Collections.singletonList("es"));
    }

    @Override
    public CompletableFuture<AppendResult> appendToStream(String streamId, long expectedVersion, NewStreamMessage... messages) {
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
    private CompletableFuture<AppendResult> appendToStreamExpectedVersionAny(StreamId streamId, NewStreamMessage[] messages) {
        final AtomicLong latestStreamVersion = new AtomicLong(0);
        return database.runAsync(tr -> {
            CompletableFuture<ReadEventResult> readEventResultFuture = readEventInternal(tr, streamId, StreamPosition.END);
            return readEventResultFuture.thenCompose(readEventResult -> {
                Subspace globalSubspace = getGlobalSubspace();
                Subspace streamSubspace = getStreamSubspace(streamId);

                latestStreamVersion.set(readEventResult.getEventNumber());

                // TODO: not a huge fan of "Version" or "StreamVersion" nomenclature/language especially when
                // eventstore bounces between those as well as position and event number
                // TODO: should timestamp be outside the loop and passed in? does it matter?
                // TODO: rather than a single tuple value how about we store two values and avoid having to a string split?
                // see how the perf compares
                for (int i = 0; i < messages.length; i++) {
                    long eventNumber = latestStreamVersion.incrementAndGet();
                    Versionstamp versionstamp = Versionstamp.incomplete(i);
                    Tuple streamSubspaceValue = packStreamSubspaceValue(streamId, messages[i], eventNumber, versionstamp);
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(eventNumber)).pack(), streamSubspaceValue.packWithVersionstamp());
                    Tuple globalSubspaceValue = Tuple.from(eventNumber + POINTER_DELIMITER + streamId.getOriginalId());
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), globalSubspaceValue.pack());
                }

                return completedFuture(tr.getVersionstamp());
            });
        })
            .thenCompose(Function.identity())
            .thenApply(trVersion -> Versionstamp.complete(trVersion, messages.length - 1))
            .thenApply(completedVersion -> new AppendResult(latestStreamVersion.get(), completedVersion));
    }

    // TODO: clean up
    private CompletableFuture<AppendResult> appendToStreamExpectedVersionNoStream(StreamId streamId, NewStreamMessage[] messages) {
        return database.runAsync(tr -> {
            CompletableFuture<ReadStreamSlice> backwardSliceFuture = readStreamBackwardsInternal(tr, streamId, StreamPosition.END, 1);
            return backwardSliceFuture.thenApplyAsync(backwardSlice -> {
                if (SliceReadStatus.STREAM_NOT_FOUND != backwardSlice.getStatus()) {
                    throw new WrongExpectedVersionException(streamId.getOriginalId(), StreamVersion.NONE);
                }
                return completedFuture(backwardSlice);
            }).thenComposeAsync(backwardSlice -> {
                Subspace globalSubspace = getGlobalSubspace();
                Subspace streamSubspace = getStreamSubspace(streamId);

                AtomicLong latestStreamVersion = new AtomicLong(-1);
                for (int i = 0; i < messages.length; i++) {
                    long eventNumber = latestStreamVersion.incrementAndGet();
                    Versionstamp versionstamp = Versionstamp.incomplete(i);
                    Tuple streamSubspaceValue = packStreamSubspaceValue(streamId, messages[i], eventNumber, versionstamp);
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(i)).pack(), streamSubspaceValue.packWithVersionstamp());
                    Tuple globalSubspaceValue = Tuple.from(eventNumber + POINTER_DELIMITER + streamId.getOriginalId());
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), globalSubspaceValue.pack());
                }
                return completedFuture(tr.getVersionstamp());
            });
        })
            .thenCompose(Function.identity())
            .thenApply(trVersion -> Versionstamp.complete(trVersion, messages.length - 1))
            .thenApply(completedVersion -> new AppendResult(messages.length - 1L, completedVersion));
    }

    // TODO: clean up
    private CompletableFuture<AppendResult> appendToStreamExpectedVersion(StreamId streamId, long expectedVersion, NewStreamMessage[] messages) {
        final AtomicLong latestStreamVersion = new AtomicLong(0);
        return database.runAsync(tr -> {
            CompletableFuture<ReadEventResult> readEventResultFuture = readEventInternal(tr, streamId, StreamPosition.END);
            // TODO: do we need to do any version/event number checking?
            return readEventResultFuture.thenCompose(readEventResult -> {
                if (!Objects.equals(expectedVersion, readEventResult.getEventNumber())) {
                    throw new WrongExpectedVersionException(streamId.getOriginalId(), expectedVersion, readEventResult.getEventNumber());
                }
                return completedFuture(readEventResult);
            }).thenCompose(readEventResult -> {
                Subspace globalSubspace = getGlobalSubspace();
                Subspace streamSubspace = getStreamSubspace(streamId);

                latestStreamVersion.set(readEventResult.getEventNumber());
                for (int i = 0; i < messages.length; i++) {
                    long eventNumber = latestStreamVersion.incrementAndGet();
                    Versionstamp versionstamp = Versionstamp.incomplete(i);
                    Tuple streamSubspaceValue = packStreamSubspaceValue(streamId, messages[i], eventNumber, versionstamp);
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, streamSubspace.subspace(Tuple.from(latestStreamVersion)).pack(), streamSubspaceValue.packWithVersionstamp());
                    Tuple globalSubspaceValue = Tuple.from(eventNumber + POINTER_DELIMITER + streamId.getOriginalId());
                    tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalSubspace.packWithVersionstamp(Tuple.from(versionstamp)), globalSubspaceValue.pack());
                }

                return completedFuture(tr.getVersionstamp());
            });
        })
            .thenCompose(Function.identity())
            .thenApply(trVersion -> Versionstamp.complete(trVersion, messages.length - 1))
            .thenApply(completedVersion -> new AppendResult(latestStreamVersion.get(), completedVersion));
    }

    @Override
    public void deleteStream(String streamId) {
        Preconditions.checkNotNull(streamId);
        // TODO: how to handle?
        // We can clear the stream subspace via clear(Range) but how to delete from global subspace?
        // would we need a scavenger process? something else?
        // add to a delete job scavenger process which contains the stream id/hash to delete
        // database.run(tr -> {);
        throw new RuntimeException("Not implemented exception");
    }

    @Override
    public SetStreamMetadataResult setStreamMetadata(String streamId, long expectedStreamMetadataVersion, Integer maxAge, Integer maxCount, String metadataJson) {
        // TODO: implement
        return null;
    }

    @Override
    public CompletableFuture<ReadAllSlice> readAllForwards(Versionstamp fromPositionInclusive, int maxCount) {
        Preconditions.checkNotNull(fromPositionInclusive);
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        return readAllForwardInternal(fromPositionInclusive, maxCount);
    }

    @Override
    public CompletableFuture<ReadAllSlice> readAllBackwards(Versionstamp fromPositionInclusive, int maxCount) {
        Preconditions.checkNotNull(fromPositionInclusive);
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        return readAllBackwardInternal(fromPositionInclusive, maxCount);
    }

    private CompletableFuture<ReadAllSlice> readAllForwardInternal(Versionstamp fromPositionInclusive, int maxCount) {
        Subspace globalSubspace = getGlobalSubspace();

        CompletableFuture<List<KeyValue>> kvs = database.readAsync(tr -> {
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
                StreamingMode.WANT_ALL
            ).asList();
        });

        ReadNextAllSlice readNext = (Versionstamp nextPosition) -> readAllForwardInternal(nextPosition, maxCount);

        return kvs.thenCompose(keyValues -> {
            if (keyValues.isEmpty()) {
                return completedFuture(new ReadAllSlice(
                    fromPositionInclusive,
                    fromPositionInclusive,
                    true,
                    ReadDirection.FORWARD,
                    readNext,
                    Empty.STREAM_MESSAGES)
                );
            }

            int limit = Math.min(maxCount, keyValues.size());
            List<CompletableFuture<ReadEventResult>> completableFutures = new ArrayList<>(limit);
            for (int i = 0; i < limit; i++) {
                KeyValue kv = keyValues.get(i);
                String value = Tuple.fromBytes(kv.getValue()).getString(0);
                String[] pointerParts = splitOnLastOccurrence(value, POINTER_DELIMITER);
                completableFutures.add(readEvent(pointerParts[1], Long.parseLong(pointerParts[0])));
            }

            // allof doesnt work with lists however if we make completablesFutures an array we run into a
            // different issue with the `.map(ReadEventResult::getEvent)` as the array needs to be bound to ?
            return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]))
                .thenCompose(ignore -> {
                    StreamMessage[] messages = completableFutures.stream()
                        .map(CompletableFuture::join)
                        .map(ReadEventResult::getEvent)
                        .toArray(size -> new StreamMessage[completableFutures.size()]);

                    Versionstamp nextPosition = maxCount >= keyValues.size()
                        ? null
                        : globalSubspace.unpack(keyValues.get(maxCount).getKey()).getVersionstamp(0);

                    return completedFuture(new ReadAllSlice(
                        fromPositionInclusive,
                        nextPosition,
                        maxCount >= keyValues.size(),
                        ReadDirection.FORWARD,
                        readNext,
                        messages)
                    );
                });
        });

    }

    private CompletableFuture<ReadAllSlice> readAllBackwardInternal(Versionstamp fromPositionInclusive, int maxCount) {
        Subspace globalSubspace = getGlobalSubspace();

        CompletableFuture<List<KeyValue>> kvs = database.readAsync(tr -> {
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
        });

        ReadNextAllSlice readNext = (Versionstamp nextPosition) -> readAllBackwardInternal(nextPosition, maxCount);

        return kvs.thenCompose(keyValues -> {
            if (keyValues.isEmpty()) {
                return completedFuture(new ReadAllSlice(
                    fromPositionInclusive,
                    fromPositionInclusive,
                    true,
                    ReadDirection.BACKWARD,
                    readNext,
                    Empty.STREAM_MESSAGES)
                );
            }

            int limit = Math.min(maxCount, keyValues.size());
            List<CompletableFuture<ReadEventResult>> completableFutures = new ArrayList<>(limit);
            for (int i = 0; i < limit; i++) {
                KeyValue kv = keyValues.get(i);
                String value = Tuple.fromBytes(kv.getValue()).getString(0);
                String[] pointerParts = splitOnLastOccurrence(value, POINTER_DELIMITER);
                completableFutures.add(readEvent(pointerParts[1], Long.parseLong(pointerParts[0])));
            }

            return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]))
                .thenCompose(ignore -> {
                    StreamMessage[] messages = completableFutures.stream()
                        .map(CompletableFuture::join)
                        .map(ReadEventResult::getEvent)
                        .toArray(size -> new StreamMessage[completableFutures.size()]);

                    Versionstamp nextPosition = maxCount >= keyValues.size()
                        ? null
                        : globalSubspace.unpack(keyValues.get(maxCount).getKey()).getVersionstamp(0);

                    return completedFuture(new ReadAllSlice(
                        fromPositionInclusive,
                        nextPosition,
                        maxCount >= keyValues.size(),
                        ReadDirection.BACKWARD,
                        readNext,
                        messages)
                    );
                });
        });

    }

    private static String[] splitOnLastOccurrence(String s, String c) {
        int lastIndexOf = s.lastIndexOf(c);
        return new String[] {s.substring(0, lastIndexOf), s.substring(lastIndexOf + 1) };
    }

    @Override
    public CompletableFuture<ReadStreamSlice> readStreamForwards(String streamId, long fromVersionInclusive, int maxCount) {
        Preconditions.checkNotNull(streamId);
        Preconditions.checkArgument(fromVersionInclusive >= -1, "fromVersionInclusive must greater than -1");
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        return database.readAsync(readTransaction -> readStreamForwardsInternal(readTransaction, new StreamId(streamId), fromVersionInclusive, maxCount));

    }

    private CompletableFuture<ReadStreamSlice> readStreamForwardsInternal(ReadTransaction tr,
                                                                          StreamId streamId,
                                                                          long fromVersionInclusive,
                                                                          int maxCount) {
        Subspace streamSubspace = getStreamSubspace(streamId);

        // add one so we can determine if we are at the end of the stream
        int rangeCount = maxCount + 1;

        KeySelector begin = fromVersionInclusive == StreamPosition.END
            ? KeySelector.lastLessOrEqual(streamSubspace.range().end)
            : KeySelector.firstGreaterOrEqual(streamSubspace.pack(fromVersionInclusive));

        CompletableFuture<List<KeyValue>> kvs = tr.getRange(
            begin,
            KeySelector.firstGreaterOrEqual(streamSubspace.range().end),
            rangeCount,
            false,
            StreamingMode.WANT_ALL).asList();

        ReadNextStreamSlice readNext = (long nextPosition) -> readStreamForwardsInternal(tr, streamId, nextPosition, maxCount);

        return kvs.thenCompose(keyValues -> {
            if (keyValues.isEmpty()) {
                return completedFuture(ReadStreamSlice.notFound(streamId, fromVersionInclusive, ReadDirection.FORWARD, readNext));
            }

            int limit = Math.min(maxCount, keyValues.size());
            StreamMessage[] messages = new StreamMessage[limit];
            for (int i = 0; i < limit; i++) {
                StreamMessage message = unpackByteTupleToStreamMessage(streamId, keyValues.get(i).getValue());
                messages[i] = message;
            }

            // TODO: review this. What should next position be if at end and when not at end?
            Tuple nextPositionValue = Tuple.fromBytes(keyValues.get(limit - 1).getValue());
            long nextPosition = nextPositionValue.getLong(5) + 1;

            return completedFuture(new ReadStreamSlice(
                streamId.getOriginalId(),
                SliceReadStatus.SUCCESS,
                fromVersionInclusive,
                nextPosition,
                0, // TODO: fix
                0L, // TODO: fix
                ReadDirection.FORWARD,
                maxCount >= keyValues.size(),
                readNext,
                messages)
            );
        });
    }

    @Override
    public CompletableFuture<ReadStreamSlice> readStreamBackwards(String streamId, long fromVersionInclusive, int maxCount) {
        Preconditions.checkNotNull(streamId);
        Preconditions.checkArgument(fromVersionInclusive >= -1, "fromVersionInclusive must greater than -1");
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        Preconditions.checkArgument(maxCount <= MAX_READ_SIZE, "maxCount should be less than %d", MAX_READ_SIZE);

        return database.readAsync(readTransaction -> {
            return readStreamBackwardsInternal(readTransaction, new StreamId(streamId), fromVersionInclusive, maxCount);
        });
    }

    private CompletableFuture<ReadStreamSlice> readStreamBackwardsInternal(ReadTransaction tr,
                                                                           StreamId streamId,
                                                                           long fromVersionInclusive,
                                                                           int maxCount) {
        Subspace streamSubspace = getStreamSubspace(streamId);

        int rangeCount = maxCount + 1;
        CompletableFuture<List<KeyValue>> kvs = tr.getRange(
            streamSubspace.pack(Tuple.from(fromVersionInclusive - maxCount)),
            // adding one because readTransaction.getRange's end range is exclusive
            streamSubspace.pack(Tuple.from(fromVersionInclusive == StreamPosition.END ? Long.MAX_VALUE : fromVersionInclusive + 1)),
            rangeCount,
            true,
            StreamingMode.WANT_ALL
        ).asList();

        ReadNextStreamSlice readNext = (long nextPosition) -> readStreamBackwardsInternal(tr, streamId, nextPosition, maxCount);

        return kvs.thenCompose(keyValues -> {
            if (keyValues.isEmpty()) {
                return completedFuture(ReadStreamSlice.notFound(streamId, fromVersionInclusive, ReadDirection.BACKWARD, readNext));
            }

            int limit = Math.min(maxCount, keyValues.size());
            StreamMessage[] messages = new StreamMessage[limit];
            for (int i = 0; i < limit; i++) {
                StreamMessage message = unpackByteTupleToStreamMessage(streamId, keyValues.get(i).getValue());
                messages[i] = message;
            }

            // TODO: review this. What should next position be if at end and when not at end?
            Tuple nextPositionValue = Tuple.fromBytes(keyValues.get(limit - 1).getValue());
            long nextPosition = nextPositionValue.getLong(5) - 1;

            return completedFuture(new ReadStreamSlice(
                streamId.getOriginalId(),
                SliceReadStatus.SUCCESS,
                fromVersionInclusive,
                nextPosition,
                0, // TODO: fix
                0L, // TODO: fix
                ReadDirection.BACKWARD,
                maxCount >= keyValues.size(),
                readNext,
                messages)
            );
        });
    }

    @Override
    public CompletableFuture<Versionstamp> readHeadPosition() {
        Subspace globalSubspace = getGlobalSubspace();

        // TODO: just call readAllBackwards by 1?
        return database.readAsync(tr -> tr.getKey(KeySelector.lastLessThan(globalSubspace.range().end)))
            .thenCompose(k -> {
                if (ByteBuffer.wrap(k).compareTo(ByteBuffer.wrap(globalSubspace.range().begin)) < 0) {
                    return completedFuture(null);
                }

                return completedFuture(globalSubspace.unpack(k).getVersionstamp(0));
            });
    }

    @Override
    public StreamMetadataResult getStreamMetadata(String streamId) {
        // TODO: implement
        return null;
    }

    @Override
    public CompletableFuture<ReadEventResult> readEvent(String stream, long eventNumber) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(stream));
        Preconditions.checkArgument(eventNumber >= -1);
        return database.readAsync(readTransaction -> readEventInternal(readTransaction, new StreamId(stream), eventNumber));
    }

    private CompletableFuture<ReadEventResult> readEventInternal(ReadTransaction tr, StreamId streamId, long eventNumber) {
        Subspace streamSubspace = getStreamSubspace(streamId);

        if (Objects.equals(eventNumber, StreamPosition.END)) {
            CompletableFuture<ReadStreamSlice> readFuture = readStreamBackwardsInternal(tr, streamId, StreamPosition.END, 1);
            return readFuture.thenCompose(read -> {
                if (read.getStatus() == SliceReadStatus.STREAM_NOT_FOUND) {
                    return completedFuture(new ReadEventResult(ReadEventStatus.NOT_FOUND, streamId.getOriginalId(), eventNumber, null));
                }

                return completedFuture(new ReadEventResult(ReadEventStatus.SUCCESS, streamId.getOriginalId(), read.getMessages()[0].getStreamVersion(), read.getMessages()[0]));
            });

        } else {
            CompletableFuture<byte[]> valueBytesFuture = tr.get(streamSubspace.pack(Tuple.from(eventNumber)));
            return valueBytesFuture.thenCompose(valueBytes -> {
                if (valueBytes == null) {
                    return completedFuture(new ReadEventResult(ReadEventStatus.NOT_FOUND, streamId.getOriginalId(), eventNumber, null));
                }

                StreamMessage message = unpackByteTupleToStreamMessage(streamId, valueBytes);
                return completedFuture(new ReadEventResult(ReadEventStatus.SUCCESS, streamId.getOriginalId(), eventNumber, message));
            });
        }
    }

    private static Tuple packStreamSubspaceValue(StreamId streamId, NewStreamMessage message, long eventNumber, Versionstamp versionstamp) {
        return Tuple.from(
            message.getMessageId(),
            streamId.getOriginalId(),
            message.getType(),
            message.getData(),
            message.getMetadata(),
            eventNumber,
            Instant.now().toEpochMilli(),
            versionstamp
        );
    }

    private static StreamMessage unpackByteTupleToStreamMessage(StreamId streamId, byte[] bytes) {
        Tuple value = Tuple.fromBytes(bytes);
        return new StreamMessage(
            streamId.getOriginalId(),
            value.getUUID(0),
            value.getLong(5),
            value.getVersionstamp(7),
            value.getLong(6),
            value.getString(2),
            value.getBytes(4),
            value.getBytes(3)
        );
    }

    private Subspace getGlobalSubspace() {
        return esSubspace.subspace(Tuple.from(EventStoreSubspaces.GLOBAL.getValue()));
    }

    private Subspace getStreamSubspace(StreamId streamId) {
        return esSubspace.subspace(Tuple.from(EventStoreSubspaces.STREAM.getValue(), streamId.getHash()));
    }

}
