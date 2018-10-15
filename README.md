# fdb-java-es

An event store layer build on top of FoundationDB

## Layout

Data is stored in two separate subspaces

### Global Subspace

globally ordered based time  
Global / [versionstamp] /

* message Id
* stream id 
* message type, 
* message data 
* message metadata 
* event number
* created Date (UTC from epoch)
                

### Stream Subspace

Stream / id (stream hash) / version /  

* message Id
* stream id 
* message type, 
* message data 
* message metadata 
* event number
* created Date (UTC from epoch)
* Versionstamp (Global position)

## Using

`EventStoreLayer` requires you pass in a FoundationDB Database as well as a DirectorySubspace. 
The DirectorySubspace is where we'll store all our events. 

Create a DirectoryLayer

``` 
new DirectoryLayer(true).createOrOpen(tr, Collections.singletonList("es")).get();
```

Create EventStoreLayer
```
EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);
```


### Append

```
EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);
String stream = "test-stream";
es.appendToStream(stream, ExpectedVersion.ANY, createNewStreamMessage());
```


### Read

Read from a stream

```
ReadStreamPage read = es.readStreamForwards("test-stream", 0, EventStoreLayer.MAX_READ_SIZE);
```

Read backwards from a stream
```
ReadStreamPage read = es.readStreamBackwards(stream, StreamPosition.END, 10);
```

Read from the all subspace
```
ReadAllPage read = es.readAllForwards(Position.START, 10);
```

Read backwards from the all subspace
```
ReadAllPage read = es.readAllBackwards(Position.END, 10);
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
