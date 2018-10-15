# Scratch Pad

TODO: Clean up! Add better description. Add details about subspace and tuple layout. Add sample code to write and read data  

TODO: Page to Slice  
TODO: javadocs - remove language to eventstore or sqlstreamstore outside of readme  
TODO: do we want to wrap Versionstamp in a "(Global )Position" domain object?     

An Eventstore Layer build on top of FoundationDB

TODO: add high level information about design/implementation  
https://github.com/bitgn/layers/blob/beb4429b9015e4c10a03cc147662f0e047491d12/go/eventstore/fdbStore.go  
fdbStore maintains two subspaces:  
Global / [versionstamp] / contract / <- vs pointer  
Aggregate / id / version / contract /  

_Mine_  
Global / [versionstamp] / stream message (id, type, content, message metadata, etc) / <- vs pointer  
Stream / id (stream hash) / version / stream message (id, type, content, message metadata, etc) /  
do we want contract aka type to be a subspace or part of the value tuple?  
FoundationDB version timestamp doesnt appear to work. likely because we only get one versionstamp per transaction  
could we use the 2 byte user/client portion? take the index of each message as the user bytes portion.  
If we did that we could only support arrays length up to a short (32,767)  

TODO: add sample to write and read


EventStore
Task<StreamEventsSlice> ReadStreamEventsForwardAsync(string stream, long start, int count, bool resolveLinkTos)
The ReadStreamEventsForwardAsync method reads the requested number of events in the order in which they were originally written to the stream from a nominated starting point in the stream.
long start - The earliest event to read (inclusive). 
553/8For the special case of the start of the stream, you should use the constant StreamPosition.Start.
```
var streamEvents = new List<ResolvedEvent>();

StreamEventsSlice currentSlice;
var nextSliceStart = StreamPosition.Start;
do
{
    currentSlice =
    _eventStoreConnection.ReadStreamEventsForward("myStream", nextSliceStart,
                                                  200, false)
                                                  .Result;

    nextSliceStart = currentSlice.NextEventNumber;

    streamEvents.AddRange(currentSlice.Events);
} while (!currentSlice.IsEndOfStream);
```


Task<AllEventsSlice> ReadAllEventsForwardAsync(Position position, int maxCount, bool resolveLinkTos);
Read all events
Event Store allows you to read events across all streams using the ReadAllEventsForwardAsync and ReadAllEventsBackwardsAsync methods. 
These work in the same way as the regular read methods, but use an instance of the global log file Position to reference events rather than the simple integer stream position described previously.
They also return an AllEventsSlice rather than a StreamEventsSlice which is the same except it uses global Positions rather than stream positions.
```
var allEvents = new List<ResolvedEvent>();

AllEventsSlice currentSlice;
var nextSliceStart = Position.Start;

do
{
    currentSlice =
        connection.ReadAllEventsForwardAsync(nextSliceStart, 200, false).Result;

    nextSliceStart = currentSlice.NextPosition;

    allEvents.AddRange(currentSlice.Events);
} while (!currentSlice.IsEndOfStream);
```

## Random Research

Links to docs, research, projects that have inspired design or implementation

- https://apple.github.io/foundationdb/getting-started-mac.html
- https://apple.github.io/foundationdb/administration.html#administration-running-foundationdb
- https://apple.github.io/foundationdb/developer-guide.html#namespace-management
- https://github.com/apple/foundationdb/blob/master/documentation/sphinx/source/kv-architecture.rst
- https://github.com/apple/foundationdb/blob/master/design/tuple.md
- https://github.com/apple/foundationdb/blob/master/documentation/sphinx/source/api-common.rst.inc
- https://github.com/apple/foundationdb/issues/627
- https://forums.foundationdb.org/t/technical-overview-of-the-database/135
- https://forums.foundationdb.org/t/versionstamp-vs-committedversion/600
- https://forums.foundationdb.org/t/is-possible-set-a-value-as-a-reference-to-another-subspace/553/8
- https://forums.foundationdb.org/t/log-abstraction-on-foundationdb/117/3
- https://forums.foundationdb.org/t/application-design-using-subspace-and-tuple/452
- https://forums.foundationdb.org/t/implementing-versionstamps-in-bindings/250
- https://forums.foundationdb.org/t/get-current-versionstamp/586/3
- https://news.ycombinator.com/item?id=16877586
- https://github.com/jaytaylor/sql-layer
- https://github.com/jaytaylor/sql-layer/blob/bebebd23f0490118c491ab4cb46b1c7d52b18d49/fdb-sql-layer-core/src/main/java/com/foundationdb/server/store/format/FullTextIndexFileStorageFormat.java
- https://github.com/jaytaylor/sql-layer/blob/bebebd23f0490118c491ab4cb46b1c7d52b18d49/fdb-sql-layer-core/src/main/java/com/foundationdb/server/service/text/Searcher.java
- https://github.com/jaytaylor/sql-layer/blob/bebebd23f0490118c491ab4cb46b1c7d52b18d49/fdb-sql-layer-core/src/main/java/com/foundationdb/server/service/text/FullTextIndexServiceImpl.java
- https://github.com/hashicorp/vault/blob/master/physical/foundationdb/foundationdb.go
- https://eventstore.org/docs/dotnet-api/reading-events/index.html
- https://www.snowflake.com/how-foundationdb-powers-snowflake-metadata-forward/
- https://github.com/abdullin/sim-cpu





//    AllEventSlice takes in ClientMessage.ResolvedEvent which is converted to ClientAPI.ResolvedEvent which takes in either a ClientMessage.ResolvedEvent or ClientMessage.ResolvedIndexedEvent
//    From ClientApi.ResolvedEvent
//    /// <summary>
//    /// The logical position of the <see cref="OriginalEvent"/>.
//    /// </summary>
//    public readonly Position? OriginalPosition;
//
//    /// <summary>
//    /// The stream name of the <see cref="OriginalEvent" />.
//    /// </summary>
//    public string OriginalStreamId { get { return OriginalEvent.EventStreamId; } }
//
//    /// <summary>
//    /// The event number in the stream of the <see cref="OriginalEvent"/>.
//    /// </summary>
//    public long OriginalEventNumber { get { return OriginalEvent.EventNumber; } }





// https://apple.github.io/foundationdb/segmented-range-reads-java.html
// https://github.com/apple/foundationdb/blob/master/documentation/sphinx/source/class-scheduling-java.rst
// https://github.com/apple/foundationdb/blob/master/recipes/java-recipes/MicroRange.java
Range r1 = streamSubspace.range();
Range r2 = streamSubspace.range(Tuple.from());
Range r3 = streamSubspace.range(Tuple.from(intToBytes(fromVersionInclusive), intToBytes(Integer.MAX_VALUE)));
Range r4 = streamSubspace.range(Tuple.from(intToBytes(fromVersionInclusive)));
Range r5 = streamSubspace.range(Tuple.from(fromVersionInclusive));
Range r6 = getStreamSubspace(streamHash.toString(), fromVersionInclusive).range();

KeySelector begin = new KeySelector(r1.begin,true, rangeCount);
KeySelector end = new KeySelector(r1.end,true, rangeCount);
KeySelector n_begin = new KeySelector(begin.getKey(),true, begin.getOffset());
//AsyncIterable<KeyValue> r = tr.getRange(n_begin, end, rangeCount, reverse, StreamingMode.WANT_ALL);

// List<KeyValue> classes = tr.getRange(Tuple.from("attends", s).range()).asList().join();
//AsyncIterable<KeyValue> r = tr.getRange(r1, rangeCount, reverse, StreamingMode.WANT_ALL);
//AsyncIterable<KeyValue> r = tr.getRange(range, rangeCount, reverse, StreamingMode.WANT_ALL);
//AsyncIterable<KeyValue> r = tr.getRange(r3, rangeCount, reverse, StreamingMode.WANT_ALL);
//AsyncIterable<KeyValue> r = tr.getRange(r4, rangeCount, reverse, StreamingMode.WANT_ALL);
//AsyncIterable<KeyValue> r = tr.getRange(r5, rangeCount, reverse, StreamingMode.WANT_ALL);
//AsyncIterable<KeyValue> r = tr.getRange(r6, rangeCount, reverse, StreamingMode.WANT_ALL);
//AsyncIterable<KeyValue> r = tr.getRange(Tuple.from(streamSubspace, fromVersionInclusive).range(), rangeCount, reverse, StreamingMode.WANT_ALL);


//    https://forums.foundationdb.org/t/get-current-versionstamp/586/3
//    public CompletableFuture<Versionstamp> getCurVersionStamp(ReadTransaction tr) {
//        AsyncIterator<KeyValue> iterator = tr.getRange(esSubspace.range(), /* limit = */ 1, /* reverse = */ true).iterator();
//        return iterator.onHasNext().thenApply(hasAny -> {
//            if (hasAny) {
//                // Get the last element from the log subspace and parse out the versionstamp
//                KeyValue kv = iterator.next();
//                return Tuple.fromBytes(kv.getKey()).getVersionstamp(0);
//            } else {
//                // Log subspace is empty
//                return null; // or a versionstamp of all zeroes if you prefer
//            }
//        });
//    }

//    Construct a versionstamp from your transaction's read version
//    This makes use of the fact that the first 8 bytes of a versionstamp are the commit version of the data associated with a record.
//    Therefore, if you know the read version of the transaction, you also know that all data in your log subspace at version v will be prefixed by a version less than or equal to v and all future data added later will be prefixed with a version greater than v.
//    So you can do something like:
//    public CompletableFuture<Versionstamp> getCurVersionStamp(ReadTransaction tr) {
//        return tr.getReadVersion().thenApply(readVersion ->
//            Versionstamp.fromBytes(ByteBuffer.allocate(Versionstamp.LENGTH)
//                .order(ByteOrder.BIG_ENDIAN)
//                .putLong(readVersion)
//                .putInt(0xffffffff)
//                .array())
//        );
//    }
