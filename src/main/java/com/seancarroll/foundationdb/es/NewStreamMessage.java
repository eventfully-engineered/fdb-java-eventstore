package com.seancarroll.foundationdb.es;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import java.util.UUID;

// TODO: check out namespace EventStore.Core.Data
// in particular
/*
  var size = Data == null ? 0 : Data.Length;
            size += Metadata == null ? 0 : Metadata.Length;
            size += eventType.Length * 2;

            if( size > TFConsts.MaxLogRecordSize - 10000)
                throw new ArgumentException("Record is too big", "data");
 */
public class NewStreamMessage {

    // TODO: define an empty byte[]

    private final UUID messageId;
    private final String type;
    // IsJson
    private final byte[] data;
    private final byte[] metadata;

    /**
     *
     * @param messageId
     * @param type
     * @param data
     */
    public NewStreamMessage(UUID messageId, String type, byte[] data) {
        this(messageId, type, data, new byte[0]);
    }

    /**
     *
     * @param messageId
     * @param type
     * @param data
     * @param metadata
     */
    public NewStreamMessage(UUID messageId, String type, byte[] data, byte[] metadata) {
        // Ensure.notNull(messageId);
        // Ensure.notNullOrEmpty(type, "type");
        // Ensure.notNullOrEmpty(jsonData, "data");
        Preconditions.checkNotNull(messageId);
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(data);

        this.messageId = messageId;
        this.type = type;
        this.data = data;
        this.metadata = metadata == null ? Empty.BYTE_ARRAY : metadata;

        // TODO: would like to be able to support large values. @see issue <insert number or link>
        int size = data == null ? 0 : data.length;
        size += metadata == null ? 0 : metadata.length;
        size += type.length() * 2;
        size += messageId.toString().length() * 2;

        // TODO: constant for value limit...does foundationdb provide it?
        if (size > 100_000) {
            // TODO: use custom exception?
            throw new IllegalArgumentException("Record is too big");
        }
    }

    public UUID getMessageId() {
        return messageId;
    }

    public String getType() {
        return type;
    }

    public byte[] getData() {
        return data;
    }

    public byte[] getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("messageId", getMessageId())
            .add("type", getType())
            .toString();
    }
}
