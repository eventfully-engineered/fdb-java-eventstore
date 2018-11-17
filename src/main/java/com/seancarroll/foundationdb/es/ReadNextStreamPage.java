package com.seancarroll.foundationdb.es;

import java.util.concurrent.CompletableFuture;

/**
 *
 *
 */
@FunctionalInterface
public interface ReadNextStreamPage {

    /**
     *
     * @param nextVersion
     * @return
     */
    CompletableFuture<ReadStreamPage> get(long nextVersion);
}
