package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.tuple.Versionstamp;

import java.util.concurrent.CompletableFuture;

/**
 * Represents an operation to read the next all slice.
 */
@FunctionalInterface
public interface ReadNextAllSlice {

    /**
     *
     * @param fromPositionInclusive
     * @return
     */
    CompletableFuture<ReadAllSlice> get(Versionstamp fromPositionInclusive);

}
