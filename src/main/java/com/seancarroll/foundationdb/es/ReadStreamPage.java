package com.seancarroll.foundationdb.es;

import com.google.common.base.MoreObjects;

import java.util.concurrent.CompletableFuture;

/**
 * From SqlStreamStore
 * EventStore has StreamEventsSlice
 * Represents the result of a read from a stream.
 */
public class ReadStreamPage {

    /**
     * The collection of messages read
     */
    private final StreamMessage[] messages;

    /**
     *
     */
    private final ReadNextStreamPage readNext;

    /**
     * The starting point (represented as a sequence number) of the read
     */
    private final long fromStreamVersion;

    /**
     * The next message version that can be read.
     * EventStore has NextEventNumber - The next event number that can be read.
     * EventStore NextExpectedVersion - The next expected version for the stream -  For example if you write to a stream at version 1, then you expect it to be at version 1 next time you write
     */
    private final long nextStreamVersion;

    /**
     * The position of the last message in the stream.
     * TODO: should use "Position" terminology here
     */
    private final long lastStreamPosition; //LastEventNumber

    /**
     * The version of the last message in the stream.
     * EventStore has LastEventNumber - The last event number in the stream.
     */
    private final long lastStreamVersion;

    /**
     * The direction of read operation.
     */
    private final ReadDirection readDirection;

    /**
     * The {@link PageReadStatus} of the read operation
     * EventStore has status property - The <see cref="SliceReadStatus"/> representing the status of this read attempt
     */
    private final PageReadStatus status;

    /**
     * The id (name) of the stream read.
     */
    private final String streamId;

    /**
     * A boolean representing Whether or not this is the end of the stream.
     */
    private final boolean isEnd;

    /**
     *
     * @param streamId - The id of the stream that was read.
     * @param status - The {@link PageReadStatus} of the read operation.
     * @param fromStreamVersion - The version of the stream that read from.
     * @param nextStreamVersion - The next message version that can be read.
     * @param lastStreamVersion - The version of the last message in the stream.
     * @param lastStreamPosition - The position of the last message in the stream.
     * @param readDirection - The direction of the read operation.
     * @param isEnd - Whether or not this is the end of the stream.
     * @param readNext - The messages read.
     */
    public ReadStreamPage(String streamId,
                          PageReadStatus status,
                          long fromStreamVersion,
                          long nextStreamVersion,
                          long lastStreamVersion,
                          long lastStreamPosition,
                          ReadDirection readDirection,
                          boolean isEnd,
                          ReadNextStreamPage readNext,
                          StreamMessage[] messages) {
        this.streamId = streamId;
        this.status = status;
        this.fromStreamVersion = fromStreamVersion;
        this.lastStreamVersion = lastStreamVersion;
        this.lastStreamPosition = lastStreamPosition;
        this.nextStreamVersion = nextStreamVersion;
        this.readDirection = readDirection;
        this.isEnd = isEnd;
        this.messages = messages == null ? Empty.STREAM_MESSAGES : messages;
        this.readNext = readNext;
    }

    public StreamMessage[] getMessages() {
        return messages;
    }

    public long getFromStreamVersion() {
        return fromStreamVersion;
    }

    public long getLastStreamPosition() {
        return lastStreamPosition;
    }

    public long getNextStreamVersion() {
        return nextStreamVersion;
    }

    public long getLastStreamVersion() {
        return lastStreamVersion;
    }

    public ReadDirection getReadDirection() {
        return readDirection;
    }

    public PageReadStatus getStatus() {
        return status;
    }

    public String getStreamId() {
        return streamId;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public CompletableFuture<ReadStreamPage> readNext() {
        // TODO: should this just return a ReadNextStreamPage instead?
        return readNext.get(nextStreamVersion);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("fromStreamVersion", getFromStreamVersion())
            .add("lastStreamVersion", getLastStreamVersion())
            .add("status", getStatus())
            .add("streamId", getStreamId())
            .add("isEnd", isEnd())
            .add("readDirection", getReadDirection())
            .toString();
    }
}

