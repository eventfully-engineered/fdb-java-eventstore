package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.tuple.Versionstamp;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public interface EventStore {

    /**
     *
     * @param streamId
     * @param expectedVersion
     * @param messages
     * @return
     */
    AppendResult appendToStream(String streamId,
                                long expectedVersion,
                                NewStreamMessage... messages) throws InterruptedException, ExecutionException;

    /**
     *
     * @param fromPositionInclusive position to start reading from. Use Position.START to start from the beginning
     * @param maxCount maximum number of events to read
     * @return An @{link ReadAllPage} presenting the result of the read. If all messages read have expired then the message collection MAY be empty.
     */
    ReadAllPage readAllForwards(Versionstamp fromPositionInclusive, int maxCount) throws InterruptedException, ExecutionException;

    /**
     *
     * @param fromPositionInclusive The position to start reading from. Use Position.END to start from the end.
     * @param maxCount maximum number of events to read
     * @return An @{link ReadAllPage} presenting the result of the read. If all messages read have expired then the message collection MAY be empty.
     */
    ReadAllPage readAllBackwards(Versionstamp fromPositionInclusive, int maxCount) throws InterruptedException, ExecutionException;

    /**
     *
     * @param streamId the stream id to read
     * @param fromVersionInclusive The version of the stream to start reading from. Use StreamVersion.Start to read from the start.
     * @param maxCount maximum number of events to read
     * @return An @{link ReadAllPage} presenting the result of the read. If all messages read have expired then the message collection MAY be empty.
     */
    ReadStreamPage readStreamForwards(String streamId,
                                      long fromVersionInclusive,
                                      int maxCount) throws InterruptedException, ExecutionException;

    /**
     *
     * @param streamId the stream id to read
     * @param fromVersionInclusive The version of the stream to start reading from. Use StreamVersion.End to read from the end
     * @param maxCount maximum number of events to read
     * @return An @{link ReadAllPage} presenting the result of the read. If all messages read have expired then the message collection MAY be empty.
     */
    ReadStreamPage readStreamBackwards(String streamId,
                                       long fromVersionInclusive,
                                       int maxCount) throws InterruptedException, ExecutionException;


    ReadEventResult readEvent(String stream, long eventNumber) throws ExecutionException, InterruptedException;

    /**
     * TODO: Do we need this? Does readAllBackwards with Position.END handle this well enough?
     * Reads the head position (the position of the very latest message) in the {@link EventStoreSubspaces#GLOBAL} subspace.
     * @return the head position
     */
    Versionstamp readHeadPosition() throws ExecutionException, InterruptedException;

    /**
     *
     * @param streamId
     * @param expectedStreamMetadataVersion
     * @param maxAge
     * @param maxCount
     * @param metadataJson
     * @return
     */
    SetStreamMetadataResult setStreamMetadata(String streamId,
                                              long expectedStreamMetadataVersion,
                                              Integer maxAge,
                                              Integer maxCount,
                                              String metadataJson);

    /**
     * Gets the stream metadata
     * @param streamId The stream ID whose metadata is to be read.
     */
    StreamMetadataResult getStreamMetadata(String streamId);

    /**
     *
     * @param streamId
     * @param expectedVersion
     */
    void deleteStream(String streamId, long expectedVersion);

    /**
     *
     * @param streamId
     * @param messageId
     */
    void deleteMessage(String streamId, UUID messageId);
}
