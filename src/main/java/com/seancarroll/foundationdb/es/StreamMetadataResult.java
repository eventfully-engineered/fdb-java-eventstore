package com.seancarroll.foundationdb.es;

import com.google.common.base.MoreObjects;

/**
 * From SqlStreamStore. See what EventStore does for Stream metadata
 *
 */
public class StreamMetadataResult {

    /**
     * The stream ID
     */
    private final String streamId;

    /**
     * The version of the metadata stream. Can be used for concurrency control
     */
    private final long metadataStreamVersion;

    private final Integer maxAge;

    private final Integer maxCount;

    private final byte[] metadata;

    public StreamMetadataResult(String streamId, long metadataStreamVersion) {
        this(streamId, metadataStreamVersion, null);
    }


    public StreamMetadataResult(String streamId, long metadataStreamVersion, byte[] metadata) {
        this(streamId, metadataStreamVersion, null, null, metadata);
    }

    /**
     *
     * @param streamId The stream ID
     * @param metadataStreamVersion The verson of the metadata stream
     * @param maxAge The max age of messages in the stream
     * @param maxCount The max count of message in the stream
     * @param metadata Custom metadata
     */
    public StreamMetadataResult(String streamId,
                                long metadataStreamVersion,
                                Integer maxAge,
                                Integer maxCount,
                                byte[] metadata) {
        this.streamId = streamId;
        this.metadataStreamVersion = metadataStreamVersion;
        this.maxAge = maxAge;
        this.maxCount = maxCount;
        this.metadata = metadata;
    }

    public String getStreamId() {
        return streamId;
    }

    public long getMetadataStreamVersion() {
        return metadataStreamVersion;
    }

    public Integer getMaxAge() {
        return maxAge;
    }

    public Integer getMaxCount() {
        return maxCount;
    }

    public byte[] getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("streamId", getStreamId())
            .add("maxAge", getMaxAge())
            .add("maxCount", getMaxCount())
            .add("metadataStreamVersion", getMetadataStreamVersion())
            .toString();
    }
}
