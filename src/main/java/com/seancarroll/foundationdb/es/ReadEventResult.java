package com.seancarroll.foundationdb.es;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 *
 */
public class ReadEventResult {

    private final ReadEventStatus status;
    private final String stream;
    private final long eventNumber;
    private final StreamMessage event;

    /**
     *
     * @param status
     * @param stream
     * @param eventNumber
     * @param event
     */
    public ReadEventResult(ReadEventStatus status,
                           String stream,
                           long eventNumber,
                           StreamMessage event) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(stream));
        this.status = status;
        this.stream = stream;
        this.eventNumber = eventNumber;
        this.event = event;
    }

    public ReadEventStatus getStatus() {
        return status;
    }

    public String getStream() {
        return stream;
    }

    public long getEventNumber() {
        return eventNumber;
    }

    public StreamMessage getEvent() {
        return event;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("status", getStatus())
            .add("stream", getStream())
            .add("eventNumber", getEventNumber())
            .add("event", getEvent())
            .toString();
    }
}
