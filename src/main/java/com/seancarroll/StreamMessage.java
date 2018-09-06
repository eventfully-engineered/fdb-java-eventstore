package com.seancarroll;

import com.google.common.base.MoreObjects;
import org.joda.time.DateTime;

import java.sql.SQLException;
import java.util.UUID;

public class StreamMessage {

    private final long position;
    private final DateTime createdUtc;
    private final UUID messageId;
    private final String jsonMetadata;
    private final int streamVersion;
    private final String streamId;
    private final String type;
    private final String jsonData;
    //private final GetJsonData getJsonData;

    public StreamMessage (
        String streamId,
        UUID messageId,
        int streamVersion,
        long position,
        DateTime createdUtc,
        String type,
        String jsonMetadata,
        String jsonData
        /*GetJsonData getJsonData*/) {
        this.streamId = streamId;
        this.messageId = messageId;
        this.streamVersion = streamVersion;
        this.position = position;
        this.createdUtc = createdUtc;
        this.type = type;
        this.jsonMetadata = jsonMetadata;
        this.jsonData = jsonData;
        //this.getJsonData = getJsonData;
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

    public String getJsonMetadata() {
        return jsonMetadata;
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

//    public String getJsonData() throws SQLException {
//        return getJsonData.get();
//    }


    public String getJsonData() {
        return jsonData;
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
