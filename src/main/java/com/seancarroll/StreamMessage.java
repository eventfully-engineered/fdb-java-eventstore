package com.seancarroll;

import com.google.common.base.MoreObjects;
import org.joda.time.DateTime;

import java.util.UUID;

public class StreamMessage {

    private final long position;
    private final DateTime createdUtc;
    private final UUID messageId;
    private final byte[] metadata;
    private final int streamVersion;
    private final String streamId;
    private final String type;
    private final byte[] data;

    public StreamMessage (
        String streamId,
        UUID messageId,
        int streamVersion,
        long position,
        DateTime createdUtc,
        String type,
        byte[] metadata,
        byte[] data) {

        this.streamId = streamId;
        this.messageId = messageId;
        this.streamVersion = streamVersion;
        this.position = position;
        this.createdUtc = createdUtc;
        this.type = type;
        this.metadata = metadata;
        this.data = data;
    }

    public long getPosition() {
        return position;
    }

    public DateTime getCreatedUtc() {
        return createdUtc;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public byte[] getMetadata() {
        return metadata;
    }

    public int getStreamVersion() {
        return streamVersion;
    }

    public String getStreamId() {
        return streamId;
    }

    public String getType() {
        return type;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("messageId", getMessageId())
            .add("streamId", getStreamId())
            .add("streamVersion", getStreamVersion())
            .add("position", getPosition())
            .add("type", getType())
            .toString();
    }
}
