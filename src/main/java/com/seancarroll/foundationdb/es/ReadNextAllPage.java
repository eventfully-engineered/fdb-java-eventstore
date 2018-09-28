package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.tuple.Versionstamp;

import java.util.concurrent.ExecutionException;

/**
 * From SqlStreamStore
 * Represents an operation to read the next all page.
 * This is a delegate
 *
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
