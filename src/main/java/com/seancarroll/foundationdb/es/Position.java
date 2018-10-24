package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Global position constants
 */
public class Position {

    public static final Versionstamp START = Versionstamp.fromBytes(ByteBuffer.allocate(Versionstamp.LENGTH)
        .order(ByteOrder.BIG_ENDIAN)
        .putLong(0L)
        .putInt(0)
        .array());

    public static final Versionstamp END = Versionstamp.fromBytes(ByteBuffer.allocate(Versionstamp.LENGTH)
        .order(ByteOrder.BIG_ENDIAN)
        .putLong(-1L)
        .putInt(-1)
        .array());

    private Position() {
        // public static fields only
    }

}
