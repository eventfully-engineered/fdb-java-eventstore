package com.eventfully.foundationdb.eventstore;

import java.util.concurrent.CompletableFuture;

/**
 *
 *
 */
@FunctionalInterface
public interface ReadNextStreamSlice {

    /**
     *
     * @param nextVersion
     * @return
     */
    CompletableFuture<ReadStreamSlice> get(long nextVersion);
}
