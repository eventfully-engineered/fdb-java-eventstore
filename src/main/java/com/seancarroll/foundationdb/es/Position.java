package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Global position constants
 */
public class Position {

    //public static final Versionstamp START = Versionstamp.incomplete(0);
    public static final Versionstamp START = Versionstamp.fromBytes(ByteBuffer.allocate(Versionstamp.LENGTH)
        .order(ByteOrder.BIG_ENDIAN)
        .putLong(0L)
        .putInt(0xffffffff)
        .array());


    // TODO: is this a valid way to do END?
    public static final Versionstamp END = Versionstamp.fromBytes(ByteBuffer.allocate(Versionstamp.LENGTH)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(Long.MAX_VALUE)
                .putInt(0xffffffff)
                .array());


    private Position() {
        // public static fields only
    }

}
