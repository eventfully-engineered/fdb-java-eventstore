package com.seancarroll;

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
    ReadAllPage get(Long fromPositionInclusive);

}
