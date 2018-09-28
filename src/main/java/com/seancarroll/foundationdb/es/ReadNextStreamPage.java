package com.seancarroll.foundationdb.es;

import java.util.concurrent.ExecutionException;

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
    ReadStreamPage get(long nextVersion) throws InterruptedException, ExecutionException;
}
