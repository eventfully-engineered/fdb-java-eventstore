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
    private final int metadataStreamVersion;

    private final Integer maxAge;

    private final Integer maxCount;

    private final String metadataJson;

    public StreamMetadataResult(String streamId, int metadataStreamVersion) {
        this(streamId, metadataStreamVersion, null);
    }


    public StreamMetadataResult(String streamId, int metadataStreamVersion, String metadataJson) {
        this(streamId, metadataStreamVersion, null, null, metadataJson);
    }

    /**
     *
     * @param streamId The stream ID
     * @param metadataStreamVersion The verson of the metadata stream
     * @param maxAge The max age of messages in the stream
     * @param maxCount The max count of message in the stream
     * @param metadataJson Custom metadata serialized as JSON
     */
    public StreamMetadataResult(String streamId,
                                int metadataStreamVersion,
                                Integer maxAge,
                                Integer maxCount,
                                String metadataJson) {
        this.streamId = streamId;
        this.metadataStreamVersion = metadataStreamVersion;
        this.maxAge = maxAge;
        this.maxCount = maxCount;
        this.metadataJson = metadataJson;
    }

    public String getStreamId() {
        return streamId;
    }

    public int getMetadataStreamVersion() {
        return metadataStreamVersion;
    }

    public Integer getMaxAge() {
        return maxAge;
    }

    public Integer getMaxCount() {
        return maxCount;
    }

    public String getMetadataJson() {
        return metadataJson;
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
