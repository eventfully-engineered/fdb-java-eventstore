package com.seancarroll;

/**
 * Not sure I completely dig this...lets see if it grows on me.
 * SqlStreamStore uses NoStream as PageReadStatus...not sure what AppendStreamExpectedVersionNoStream.sql does...does it start a stream?
 * Also do I want to make enum?
 *
 * The version at which we currently expect the stream to be in order that an optimistic concurrency check can be performed. This should either be a positive integer, or one of the constants `ExpectedVersion.NoStream`, `ExpectedVersion.EmptyStream`, or to disable the check, `ExpectedVersion.Any`. See here for a broader discussion of this.
 * http://docs.geteventstore.com/dotnet-api/3.9.0/optimistic-concurrency-and-idempotence/
 *
 * ExpectedVersion.Any	This disables the optimistic concurrency check.
 * ExpectedVersion.NoStream	This specifies the expectation that target stream does not yet exist.
 * ExpectedVersion.EmptyStream	This specifies the expectation that the target stream has been explicitly created, but does not yet have any user events written in it.
 * ExpectedVersion.StreamExists	This specifies the expectation that the target stream or its metadata stream has been created, but does not expect the stream to be at a specific event number.
 * Any other integer value	The event number that you expect the stream to currently be at.
 *
 *
 */
public final class ExpectedVersion {

    /**
     * The Stream should exist and should be Empty.
     * If it does not exist or is not empty treat that as a concurrency problem
     */
    public static final int EMPTY_STREAM = 0;

    /**
     * Stream does not exist
     */
    public static final int NO_STREAM = -1;


    /**
     * Any version
     */
    public static final int ANY = -2;

    /**
     * The stream should exist.
     * If it or a metadata stream does not exist treat that as a concurrency problem.
     */
    public static final int STREAM_EXISTS = -4;


    private ExpectedVersion() {
        // static constants only
    }

}

