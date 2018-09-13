package com.seancarroll.foundationdb.es;

import com.google.common.base.MoreObjects;

/**
 * This is from SqlStreamStore
 * EventStore has AllEventsSlice which contains ResolvedEvent[] events.
 * ResolvedEvent represents a single event or an resolved link event. It essentially wraps RecordedEvent event and RecordedEvent link
 *
 * Represents the result of a read of all streams.
 */
public class ReadAllPage {

    /**
     * Represents the position where this page was read from
     */
    private final long fromPosition;

    /**
     * Represents the position where the next page should be read from
     */
    private final long nextPosition;

    /**
     * True if page reached end of the all stream at the time of reading. Otherwise false
     */
    private final boolean isEnd;

    /**
     * The direction of the read request
     */
    private final ReadDirection readDirection;

    private final ReadNextAllPage readNext;

    /**
     * The collection of {@link StreamMessage}s returned as part of the read
     */
    private final StreamMessage[] messages;

    /**
     * Initializes a new instance of {@link ReadAllPage }
     * @param fromPosition A long representing the position where this page was read from.
     * @param nextPosition A long representing the position where the next page should be read from.
     * @param isEnd True if page reach end of the all stream at time of reading. Otherwise false.
     * @param readDirection The direction of the the read request.
     * @param messages The collection messages read.
     */
    public ReadAllPage(
        long fromPosition,
        long nextPosition,
        boolean isEnd,
        ReadDirection readDirection,
        ReadNextAllPage readNext,
        StreamMessage[] messages) {
        this.fromPosition = fromPosition;
        this.nextPosition = nextPosition;
        this.isEnd = isEnd;
        this.readDirection = readDirection;
        this.readNext = readNext;
        this.messages = messages == null ? Empty.STREAM_MESSAGES : messages;
    }

    /**
     * Reads the next page
     * @return
     */
    public ReadAllPage readNext() {
        return readNext.get(nextPosition);
    }


    public long getFromPosition() {
        return fromPosition;
    }

    public long getNextPosition() {
        return nextPosition;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public ReadDirection getReadDirection() {
        return readDirection;
    }

    public StreamMessage[] getMessages() {
        return messages;
    }


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("fromPosition", getFromPosition())
            .add("nextPosition", getNextPosition())
            .add("isEnd", isEnd())
            .add("readDirection", getReadDirection())
            .add("streamMessageCount", getMessages().length)
            .toString();
    }
}
