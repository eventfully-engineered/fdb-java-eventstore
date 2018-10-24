package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.tuple.Versionstamp;

import java.util.concurrent.ExecutionException;

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
    ReadAllPage get(Versionstamp fromPositionInclusive) throws InterruptedException, ExecutionException;

}
