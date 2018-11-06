package com.seancarroll.foundationdb.es;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

// TODO: I dont really like the name

/**
 *
 */
public class StreamId {

    private final String originalId;
    private final String hash;

    /**
     *
     * @param id
     */
    public StreamId(String id) {
        Preconditions.checkNotNull(id);
        if (StringUtils.containsWhitespace(id)) {
            throw new IllegalArgumentException("value must not contain whitespace");
        }
        this.originalId = id;
        this.hash = Hashing.murmur3_128().hashString(id, UTF_8).toString();
    }

    /**
     * Original id provided by the caller
     * @return
     */
    public String getOriginalId() {
        return originalId;
    }

    /**
     *
     * @return murmur3 128 bit hash of the id
     */
    public String getHash() {
        return hash;
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalId, hash);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StreamId other = (StreamId) obj;
        return originalId.equals(other.originalId) && hash.equals(other.hash);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("originalId", getOriginalId())
            .add("hash", getHash())
            .toString();
    }
}
