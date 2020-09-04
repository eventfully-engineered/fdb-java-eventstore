package com.eventfully.foundationdb.eventstore;

import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Global position constants
 */
public class Position {

    /**
     * Position representing the start of the global subspace
     */
    public static final Versionstamp START = Versionstamp.fromBytes(ByteBuffer.allocate(Versionstamp.LENGTH)
        .order(ByteOrder.BIG_ENDIAN)
        .putLong(0L)
        .putInt(0)
        .array());

    /**
     * Position representing the end of the global subspace
     */
    public static final Versionstamp END = Versionstamp.fromBytes(ByteBuffer.allocate(Versionstamp.LENGTH)
        .order(ByteOrder.BIG_ENDIAN)
        .putLong(-1L)
        .putInt(-1)
        .array());

    private Position() {
        // public static fields only
    }

}
