# fdb-java-es 

[![Build Status](https://travis-ci.org/seancarroll/fdb-java-es.svg?branch=master)](https://travis-ci.org/seancarroll/fdb-java-es)
[![codecov](https://codecov.io/gh/seancarroll/fdb-java-es/branch/master/graph/badge.svg)](https://codecov.io/gh/seancarroll/fdb-java-es)
[![Maintainability](https://api.codeclimate.com/v1/badges/a752f86172d4399bb46b/maintainability)](https://codeclimate.com/github/seancarroll/fdb-java-es/maintainability)
[![Sonarcloud Status](https://sonarcloud.io/api/project_badges/measure?project=seancarroll_fdb-java-es&metric=alert_status)](https://sonarcloud.io/dashboard?id=seancarroll_fdb-java-es)

An event store layer build on top of FoundationDB

## Layout

Data is stored in two separate subspaces

### Global Subspace

Globally ordered based time  
Global / [versionstamp] /

Global subspace tuple value is a pointer to an event in the stream subspace stored in the format of `<event number>@<stream id>`. 
When reading from the global subspace via the `readAll*` methods we resolve the pointer and return the message from the 
stream subspace
                

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

Create EventStoreLayer with default "es" DirectorySubspace 
```
EventStoreLayer es = EventStoreLayer.getDefault(db);
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
ReadStreamSlice read = es.readStreamForwards("test-stream", 0, EventStoreLayer.MAX_READ_SIZE);
```

Read backwards from a stream
```
ReadStreamSlice read = es.readStreamBackwards(stream, StreamPosition.END, 10);
```

Read from the all subspace
```
ReadAllSlice read = es.readAllForwards(Position.START, 10);
```

Read backwards from the all subspace
```
ReadAllSlice read = es.readAllBackwards(Position.END, 10);
```

Build / CI / Plugins 
- Travis CI [https://travis-ci.org/seancarroll/fdb-java-es]
- SonarCloud [https://sonarcloud.io/dashboard?id=seancarroll_fdb-java-es]
- CodeCov [https://codecov.io/gh/seancarroll/fdb-java-es]
- Coveralls [https://coveralls.io/github/seancarroll/fdb-java-es]
- Code Climate [https://codeclimate.com/repos/5bf0b88184303f02850000c0/settings/test_reporter]
- Snyk [https://app.snyk.io/org/seancarroll/projects]
