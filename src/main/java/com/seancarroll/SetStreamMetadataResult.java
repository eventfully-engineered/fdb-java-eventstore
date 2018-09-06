package com.seancarroll;

import com.google.common.base.MoreObjects;

/**
 * Represents the result of setting a stream's metadata
 *
 */
public class SetStreamMetadataResult {

    /**
     * The current version of the stream at the time the metadata was written.
     */
    public final int currentVersion;

    /**
     * Initializes a new instance of the {@link SetStreamMetadataResult}.
     * @param currentVersion
     */
    public SetStreamMetadataResult(int currentVersion) {
        this.currentVersion = currentVersion;
    }

    public int getCurrentVersion() {
        return currentVersion;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("currentVersion", getCurrentVersion())
            .toString();
    }
}
