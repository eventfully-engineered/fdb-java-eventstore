package com.seancarroll;

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
