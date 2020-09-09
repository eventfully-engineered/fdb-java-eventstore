package com.eventfully.foundationdb.eventstore;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.UUID;

/**
 * Represents a message to be appended to a stream.
 */
public class NewStreamMessage {

    private final UUID messageId;
    private final String type;
    // TODO: do we want to add an isJson field?
    private final byte[] data;
    private final byte[] metadata;

    /**
     *
     * @param messageId
     * @param type
     * @param data
     */
    public NewStreamMessage(UUID messageId, String type, byte[] data) {
        this(messageId, type, data, Empty.BYTE_ARRAY);
    }

    /**
     *
     * @param messageId
     * @param type
     * @param data
     * @param metadata
     */
    public NewStreamMessage(UUID messageId, String type, byte[] data, byte[] metadata) {
        Preconditions.checkNotNull(messageId);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(type));
        Preconditions.checkNotNull(data);

        this.messageId = messageId;
        this.type = type;
        this.data = data;
        this.metadata = metadata == null ? Empty.BYTE_ARRAY : metadata;

        // TODO: would like to be able to support large values.
        // @see issue https://github.com/eventfully-engineered/fdb-java-eventstore/issues/1
        int size = data.length;
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
