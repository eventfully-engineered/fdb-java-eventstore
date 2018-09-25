package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.base.MoreObjects;

import java.util.UUID;

public class StreamMessage {

    private final Versionstamp position;
    private final Long createdUtc;
    private final UUID messageId;
    private final byte[] metadata;
    private final int streamVersion;
    private final String streamId;
    private final String type;
    private final byte[] data;

    public StreamMessage(String streamId,
                         UUID messageId,
                         int streamVersion,
                         Versionstamp position,
                         Long createdUtc,
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

    public Versionstamp getPosition() {
        return position;
    }

    /**
     * A long representing the milliseconds since the epoch when the was created in the system
     * @return
     */
    public Long getCreatedUtc() {
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
            .add("createdUtc", getCreatedUtc())
            .toString();
    }
}
