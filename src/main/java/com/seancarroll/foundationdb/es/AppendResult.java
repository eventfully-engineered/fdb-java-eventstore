package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.base.MoreObjects;

/**
 * Represents the result returned after appending to a stream
 *
 */
public class AppendResult {

    private Integer maxCount;

    /**
     * The current version of the stream after the append operation was performed
     *
     */
    private final int currentVersion;

    /**
     * The current position of the stream after the append operation was performed
     */
    private final Versionstamp currentPosition;

    /**
     * Constructs new {@link AppendResult}
     * @param currentVersion The current version of the stream after the append operation was performed.
     * @param currentPosition The current position of the stream after the append operation was performed.
     */
    public AppendResult(int currentVersion, Versionstamp currentPosition) {
        this(null, currentVersion, currentPosition);
    }

    /**
     * Constructs new {@link AppendResult}
     * @param currentVersion The current version of the stream after the append operation was performed.
     * @param currentPosition The current position of the stream after the append operation was performed.
     */
    public AppendResult(Integer maxCount, int currentVersion, Versionstamp currentPosition) {
        this.maxCount = maxCount;
        this.currentVersion = currentVersion;
        this.currentPosition = currentPosition;
    }

    public Integer getMaxCount() {
        return maxCount;
    }

    public int getCurrentVersion() {
        return currentVersion;
    }

    public Versionstamp getCurrentPosition() {
        return currentPosition;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("maxCount", getMaxCount())
            .add("currentVersion", getCurrentVersion())
            .add("currentPosition", getCurrentPosition())
            .toString();
    }
}
