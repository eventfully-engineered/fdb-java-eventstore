package com.seancarroll.foundationdb.es;

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
    ReadStreamPage get(int nextVersion);
}
