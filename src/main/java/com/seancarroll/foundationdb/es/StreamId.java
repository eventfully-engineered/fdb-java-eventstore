package com.seancarroll.foundationdb.es;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

// TODO: I dont really like the name
public class StreamId {

    private final String id;
    private final String hash;

    public StreamId(String id) {
        Preconditions.checkNotNull(id);
        if (StringUtils.containsWhitespace(id)) {
            throw new IllegalArgumentException("value must not contain whitespace");
        }
        this.id = id;

        // new String(Hashing.murmur3_128().hashString(originalId, StandardCharsets.UTF_8).asBytes(), StandardCharsets.UTF_8)
        this.hash = Hashing.murmur3_128().hashString(id, UTF_8).toString();
    }

    /**
     * Original id provided by the caller
     * @return
     */
    public String getId() {
        return id;
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
        return Objects.hash(id, hash);
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
        return id.equals(other.id) && hash.equals(other.hash);
    }


}
