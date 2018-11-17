package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.tuple.Versionstamp;

import java.util.concurrent.CompletableFuture;

/**
 * Represents an operation to read the next all page.
 */
@FunctionalInterface
public interface ReadNextAllPage {

    /**
     *
     * @param fromPositionInclusive
     * @return
     */
    CompletableFuture<ReadAllPage> get(Versionstamp fromPositionInclusive);

}
