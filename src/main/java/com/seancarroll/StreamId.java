package com.seancarroll;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

// TODO: I dont really like the name
public class StreamId {

    // TODO: how about we juse id and hash?
    private final String originalId;
    private final String id;

    public StreamId(String originalId) {
        Preconditions.checkNotNull(originalId);
        if (StringUtils.containsWhitespace(originalId)) {
            throw new IllegalArgumentException("value must not contain whitespace");
        }
        this.originalId = originalId;

        // new String(Hashing.murmur3_128().hashString(originalId, StandardCharsets.UTF_8).asBytes(), StandardCharsets.UTF_8)
        this.id = Hashing.murmur3_128().hashString(originalId, UTF_8).toString();
    }

    public String getOriginalId() {
        return originalId;
    }

    public String getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalId, id);
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
        return originalId.equals(other.originalId) && id.equals(other.id);
    }


}
