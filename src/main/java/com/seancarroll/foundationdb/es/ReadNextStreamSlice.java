package com.seancarroll.foundationdb.es;

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
