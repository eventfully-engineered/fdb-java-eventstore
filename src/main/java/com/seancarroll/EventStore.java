package com.seancarroll;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;

import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public interface EventStore {

    AppendResult appendToStream(
        String streamId,
        int expectedVersion,
        NewStreamMessage[] messages);


    void deleteStream(String streamId, int expectedVersion);

    void deleteMessage(String streamId, UUID messageId);

    SetStreamMetadataResult setStreamMetadata(
        String streamId,
        int expectedStreamMetadataVersion, // = ExpectedVersion.Any,
        Integer maxAge,
        Integer maxCount,
        String metadataJson);


    /**
     *
     * @param fromPositionInclusive position to start reading from. Use Position.START to start from the beginning
     * @param maxCount maximum number of events to read
     * @param prefetch Prefetches the message data as part of the page read. This means a single request to the server but a higher payload size.
     * @return An @{link ReadAllPage} presenting the result of the read. If all messages read have expired then the message collection MAY be empty.
     */
    ReadAllPage readAllForwards(long fromPositionInclusive, int maxCount, boolean prefetch) throws SQLException;

    /**
     *
     * @param fromPositionInclusive The position to start reading from. Use Position.END to start from the end.
     * @param maxCount maximum number of events to read
     * @param prefetch Prefetches the message data as part of the page read. This means a single request to the server but a higher payload size.
     * @return An @{link ReadAllPage} presenting the result of the read. If all messages read have expired then the message collection MAY be empty.
     */
    ReadAllPage readAllBackwards(long fromPositionInclusive, int maxCount, boolean prefetch) throws SQLException;

    /**
     *
     * @param streamId the stream id to read
     * @param fromVersionInclusive The version of the stream to start reading from. Use StreamVersion.Start to read from the start.
     * @param maxCount maximum number of events to read
     * @param prefetch Prefetches the message data as part of the page read. This means a single request to the server but a higher payload size.
     * @return An @{link ReadAllPage} presenting the result of the read. If all messages read have expired then the message collection MAY be empty.
     */
    ReadStreamPage readStreamForwards(
        String streamId,
        int fromVersionInclusive,
        int maxCount,
        boolean prefetch) throws SQLException;

    /**
     *
     * @param streamId the stream id to read
     * @param fromVersionInclusive The version of the stream to start reading from. Use StreamVersion.End to read from the end
     * @param maxCount maximum number of events to read
     * @param prefetch Prefetches the message data as part of the page read. This means a single request to the server but a higher payload size.
     * @return An @{link ReadAllPage} presenting the result of the read. If all messages read have expired then the message collection MAY be empty.
     */
    ReadStreamPage readStreamBackwards(String streamId,
                                       int fromVersionInclusive,
                                       int maxCount,
                                       boolean prefetch) throws SQLException;


    /**
     * Reads the head position (the position of the very latest message).
     * @return the head position
     */
    // TODO: not sure if its a great idea to require client to pass in a transaction or not
    Long readHeadPosition(Transaction tr, Subspace subspace) throws ExecutionException, InterruptedException;

    /**
     * Gets the stream metadata
     * @param streamId The stream ID whose metadata is to be read.
     */
    StreamMetadataResult getStreamMetadata(String streamId) throws SQLException;


}
