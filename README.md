Copycat
=======

[![Build Status](https://travis-ci.org/kuujo/copycat.png)](https://travis-ci.org/kuujo/copycat)

Copycat is an extensible log-based distributed coordination framework for Java 8 built on the
[Raft consensus protocol](https://raftconsensus.github.io/).

#### [User Manual](#user-manual)
#### [Javadocs][Javadoc]

Copycat is a strongly consistent embedded distributed coordination framework built on the
[Raft consensus protocol](https://raftconsensus.github.io/). Copycat exposes a set of high level APIs with tools to
solve a variety of distributed systems problems including:
* [Distributed coordination tools](#distributed-coordination)
* [Distributed collections](#distributed-collections)
* [Distributed atomic variables](#distributed-atomic-variables)

Additionally, Copycat is built on a series of low-level libraries that form its consensus algorithm. All base
libraries are provided as standalone modules wherever possible, so users can use many of the following
components of the Copycat project independent of higher level libraries:
* A low-level [I/O & serialization framework](#io-serialization)
* A generalization of [asynchronous client-server messaging](#transport) with a [Netty implementation](#nettytransport)
* A low-level [self-cleaning log](#storage) designed for use with the [Raft consensus algorithm][Raft]
* A feature-complete [implementation of the Raft consensus algorithm](#raft-consensus-algorithm)
* A lightweight [Raft client](#raftclient) including support for linearizable operations via [sessions](#sessions)

**Copycat is still undergoing heavy development and testing and is therefore not recommended for production!**

[Jepsen](https://github.com/aphyr/jepsen) tests are [currently being developed](http://github.com/jhalterman/copycat-jepsen)
to verify the stability of Copycat in an unreliable distributed environment. There is still work to be done, and Copycat
will not be fully released until significant testing is done both via normal testing frameworks and Jepsen. In the meantime,
Copycat snapshots will be pushed, and a beta release of Copycat is expected within the coming weeks. Follow the project for
updates!

*Copycat requires Java 8*

User Manual
===========

Documentation is still under active development. The following documentation is loosely modeled on the structure
of modules as illustrated in the [Javadoc][Javadoc]. Docs will be updated frequently until a release, so check
back for more! If you would like to request specific documentation, please
[submit a request](http://github.com/kuujo/copycat/issues)

1. [Introduction](#introduction)
   * [Project structure](#project-structure)
   * [Dependencies](#dependencies)
   * [The Copycat API](#the-copycat-api)
      * [CopycatServer](#copycatserver)
      * [CopycatClient](#copycatclient)
   * [Thread model](#thread-model)
      * [Asynchronous API usage](#asynchronous-api-usage)
      * [Synchronous API usage](#synchronous-api-usage)
   * [Nodes](#nodes)
   * [Resources](#resources)
      * [Persistence model](#persistence-model)
      * [Consistency model](#consistency-model)
1. [Distributed resources](#distributed-resources)
   * [Distributed collections](#distributed-collections)
      * [DistributedSet](#distributedset)
      * [DistributedMap](#distributedmap)
   * [Distributed atomic variables](#distributed-atomic-variables)
      * [DistributedAtomicValue](#distributedatomicvalue)
   * [Distributed coordination](#distributed-coordination)
      * [DistributedLock](#distributedlock)
      * [DistributedLeaderElection](#distributedleaderelection)
      * [DistributedTopic](#distributedtopic)
   * [Custom resources](#custom-resources)
1. [I/O & Serialization](#io-serialization)
   * [Buffers](#buffers)
      * [Bytes](#bytes)
      * [Buffer pools](#buffer-pools)
   * [Serialization](#serialization)
      * [Serializer](#serializer)
      * [Pooled object deserialization](#pooled-object-deserialization)
      * [Serializable type resolution](#serializable-type-resolution)
      * [CopycatSerializable](#copycatserializable)
      * [TypeSerializer](#typeserializer)
   * [Storage](#storage)
      * [Log](#log)
      * [Log cleaning](#log-cleaning)
   * [Transports](#transports)
1. [Raft consensus algorithm](#raft-consensus-algorithm)
   * [RaftServer](#raftserver)
   * [Server lifecycle](#server-lifecycle)
   * [Commands](#commands)
      * [Command persistence](#command-persistence)
   * [Queries](#queries)
      * [Query consistency](#query-consistency)
   * [State machines](#state-machines)
      * [StateMachineContext](#statemachinecontext)
      * [StateMachineExecutor](#statemachineexecutor)
      * [Commits](#commits)
      * [Sessions](#sessions)
      * [Commit cleaning](#commit-cleaning)
   * [RaftClient](#raftclient)
      * [Session](#session)
1. [Miscellaneous](#miscellaneous)
   * [Builders](#builders)
   * [Listeners](#listeners)
   * [Contexts](#contexts)

## Introduction

Copycat is a framework for consistent distributed coordination. At the core of Copycat is a generic implementation
of the Raft consensus protocol. On top of Raft, Copycat provides a high level path-based API for creating and
interacting with arbitrary replicated state machines such as maps, sets, locks, or user-defined resources. Resources
can be created and operated on by any node in the cluster.

Copycat clusters consist of at least one [server](#copycatserver) and any number of [clients](#copycatclient).
For fault-tolerance, it is recommended that each Copycat cluster have 3 or 5 servers, and the number of servers should
always be odd in order to achieve the greatest level of fault-tolerance.

All network driven interfaces in Copycat are asynchronous and make heavy use of Java 8's `CompletableFuture`.
At the core of Copycat is the [Copycat API][Copycat], which is exposed by both servers and clients.

### Project structure

Copycat is designed as a series of libraries that combine to form a high-level framework for managing consistent
state in a distributed system. The project currently consists of 14 modules, each of which implements a portion
of the framework's functionality. The components of the project are composed hierarchically, so lower level
components can be used independently of most other modules.

A rough outline of Copycat's project hierarchy is as follows (from high-level to low-level):

* [Resources][Resource]
   * [Distributed collections][collections] (artifact ID: `copycat-collections`)
   * [Distributed atomic variables][atomic] (artifact ID: `copycat-atomic`)
   * [Distributed coordination tools][coordination] (artifact ID: `copycat-coordination`)
* [Copycat API][copycat] (artifact ID: `copycat`)
   * [Copycat Client][CopycatClient]
   * [Copycat Server][CopycatServer]
   * [Resource API][Resource]
* [Raft Consensus Algorithm][raft]
   * [Raft Protocol][protocol] (artifact ID: `copycat-protocol`)
   * [Raft Client][RaftClient] (artifact ID: `copycat-client`)
   * [Raft Server][RaftServer] (artifact ID: `copycat-server`)
* [I/O & Serialization][io]
   * [Buffer][io] (artifact ID: `copycat-io`)
   * [Serializer][serializer] (artifact ID: `copycat-io`)
   * [Transport][transport] (artifact ID: `copycat-transport`)
      * [Local transport][LocalTransport] (artifact ID: `copycat-local`)
      * [Netty transport][NettyTransport] (artifact ID: `copycat-netty`)
   * [Storage][storage] (artifact ID: `copycat-storage`)
* [Utilities][utilities] (artifact ID: `copycat-common`)
   * [Builder][Builder]
   * [Listener][Listener]
   * [Context][Context]

### Dependencies

Copycat is designed to ensure that different components of the project ([resources](#resources),
[Raft](#raft-consensus-algorithm), [I/O](#io-serialization), etc) can work independently of one another
and with minimal dependencies. To that end, Copycat only depends on non-JDK libraries where dependencies are
obvious, such as is the case with the [NettyTransport][NettyTransport].

Copycat provides an all-encompassing dependency - `copycat-all` - which provides all base modules, transport,
and [resource](#resources) dependencies.

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-all</artifactId>
  <version>0.6.0-SNAPSHOT</version>
</dependency>
```

If `copycat-all` is just not your style, to add Copycat's high-level API as a dependency to your Maven
project add the `copycat` dependency:

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat</artifactId>
  <version>0.6.0-SNAPSHOT</version>
</dependency>
```

Additionally, in order to facilitate communication between [clients](#copycatclient) and [servers](#copycatserver)
you must add a [Transport](#transport) dependency. Typically, the [NettyTransport][NettyTransport] will suffice
for most use cases:

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-netty</artifactId>
  <version>0.6.0-SNAPSHOT</version>
</dependency>
```

Finally, to add specific [resources](#resources) as dependencies, add one of the resource modules:

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-collections</artifactId>
  <version>0.6.0-SNAPSHOT</version>
</dependency>
```

### The Copycat API

Copycat provides a high-level path-based API for creating and operating on custom replicated state machines.
Additionally, Copycat provides a number of custom [resources](#resources) to aid in common distributed
coordination tasks:
* [Distributed atomic variables](#distributed-atomic-variables)
* [Distributed collections](#distributed-collections)
* [Distributed coordination tools](#distributed-coordination)

Resources are managed via a [Copycat][Copycat] instance which is shared by both [clients](#copycatclient) and
[servers](#copycatserver). This allows Copycat clients and servers to be embedded in applications that don't
care about the context. Resources can be created and operated on regardless of whether the local `Copycat`
instance is a [CopycatClient][CopycatClient] or [CopycatServer][CopycatServer].

#### CopycatServer

The [CopycatServer][CopycatServer] is a [Copycat][Copycat] implementation that is responsible for receiving
creating and managing [resources](#resources) on behalf of other clients and servers and receiving, persisting,
and replicating state changes for existing resources.

Because servers are responsible for persisting and replicating resource state changes, they require
more configuration than [clients](#copycatclient).

To create a `CopycatServer`, first you must create a [Transport](#transport) via which the server will communicate
with other clients and servers:

```java
Transport transport = new NettyTransport();
```

The [Transport][Transport] provides the mechanism through which servers communicate with one another and
with clients. It is essential that all clients and servers configure the same transport.

Once the transport is configured, the server must be provided with a list of members to which to connect.
Cluster membership information is provided by configuring a `Members` list.

```java
Members members = Members.builder()
  .addMember(Member.builder()
    .withId(1)
    .withHost("123.456.789.1")
    .withPort(5555)
    .build())
  .addMember(Member.builder()
    .withId(2)
    .withHost("123.456.789.2")
    .withPort(5555)
    .build())
  .addMember(Member.builder()
    .withId(3)
    .withHost("123.456.789.3")
    .withPort(5555)
    .build())
  .build();
```

Each member in the `Members` list must be assigned a unique `id` that remains consistent across
all clients and servers in the cluster, and the local server must be listed in the `Members` list.
In other words, if host `123.456.789.1` is member `1` on one server, it must be listed as member
`1` on all servers.

Finally, the `CopycatServer` is responsible for persisting [resource](#resources) state changes.
To do so, the underlying [Raft](#raft-consensus-algorithm) writes state changes to a [Log][Log].
Users must provide a [Storage][Storage] object which specifies how the underlying `Log` should
be created and managed.

To create a `Storage` object, use the storage [Builder](#builders):

```java
Storage storage = Storage.builder()
  .withDirectory("logs")
  .withStorageLevel(StorageLevel.DISK)
  .build();
```

The configured `StorageLevel` defines how state changes should be persisted. The storage module
can be configured with one of two storage levels:

* `StorageLevel.MEMORY` - Stores state changes in an off-heap, in-memory [Buffer](#buffers)
* `StorageLevel.DISK` - Stores state changes in a series of on-disk file buffers

Finally, with the [Transport][Transport], [Storage][Storage], and `Members` configured, create
the [CopycatServer][CopycatServer] with the server [Builder](#builders) and `open()` the server:

```java
Copycat copycat = CopycatServer.builder()
  .withMemberId(1)
  .withMembers(members)
  .withTransport(transport)
  .withStorage(storage)
  .build();

copycat.open().thenRun(() -> {
  System.out.println("Server started!");
});
```

Once created, the server can be used as any `Copycat` instance to create and operate on[nodes](#nodes)
and [resources](#resources).

Internally, the `CopycatServer` wraps a [RaftClient][RaftClient] and [RaftServer][RaftServer] to
communicate with other members of the cluster. For more information on the specific implementation
of `CopycatServer` see the [RaftClient](#raftclient) and [RaftServer](#raftserver) documentation.

#### CopycatClient

The [CopycatClient][CopycatClient] is a [Copycat][Copycat] implementation that manages and operates
on [resources](#resources) by communicating with a remote cluster of [servers](#copycatserver).

To create a `CopycatClient`, use the client [Builder](#builders) and provide a [Transport][Transport]
and a list of `Members` to which to connect:

```java
Copycat copycat = CopycatClient.builder()
  .withTransport(new NettyTransport())
  .withMembers(Members.builder()
    .addMember(Member.builder()
      .withId(1)
      .withHost("123.456.789.1")
      .withPort(5555)
      .build())
    .addMember(Member.builder()
      .withId(2)
      .withHost("123.456.789.2")
      .withPort(5555)
      .build())
    .addMember(Member.builder()
      .withId(3)
      .withHost("123.456.789.3")
      .withPort(5555)
      .build())
    .build())
  .build();
```

The provided `Members` list does not have to be representative of the full list of active servers.
Users must simply provide enough `Member`s to be able to successfully connect to at least one
correct server.

Once the client has been created, open a new session to the Copycat cluster by calling the `open()`
method:

```java
copycat.open().thenRun(() -> {
  System.out.println("Client connected!");
});
```

The `CopycatClient` wraps a [RaftClient][RaftClient] to communicate with servers internally.
For more information on the client implementation see the [Raft client documentation](#raftclient).

### Thread model

Copycat is designed to be used in an asynchronous manner that provides easily understood
guarantees for users. All usage of asynchronous APIs such as `CompletableFuture` are carefully
orchestrated to ensure that various callbacks are executed in a deterministic manner. To that
end, Copycat provides the following single guarantee:

* Callbacks for any given object are guaranteed to always be executed on the same thread

#### Asynchronous API usage

Copycat's API makes heavy use of Java 8's [CompletableFuture][CompletableFuture] for asynchronous
completion of method calls. The asynchronous API allows users to execute multiple operations concurrently
instead of blocking on each operation in sequence. For information on the usage of `CompletableFuture`
[see the CompletableFuture documentation][CompletableFuture].

Most examples in the following documentation will assume asynchronous usage of the `CompletableFuture`
API. See [synchronous API usage](#synchronous-api-usage) for examples of how to use the API synchronously.

#### Synchronous API usage

Copycat makes heavy use of Java 8's [CompletableFuture][CompletableFuture] in part because it allows
users to easily block on asynchronous method calls. The following documentation largely portrays
asynchronous usage. To block and wait for a `CompletableFuture` result instead of registering an
asynchronous callback, simply use the `get()` or `join()` methods.

```java
// Get the "foo" key from a map
CompletableFuture<String> future = map.get("foo");

// Block to wait for the result
String result = future.get();
```

### Nodes

The high level `Copycat` API provides a hierarchical file-system-like interface that allows users to define arbitrary
named resources. At each path, the user creates a `Node` object. A `Node` is essentially a representation of a single
replicated state machine. Each Copycat cluster can manage multiple state machines.

To create a `Node`, use the `Copycat.create` method:

```java
copycat.create("/test-node").thenAccept(node -> {
  System.out.println("Successfully created node: " + node.path());
});
```

The node can then be used to [create and manage a resource](#resources) associated with the node, or create
child nodes:

```java
Node child = node.create("child").get();
```

### Resources

The true power of Copycat comes through provided and custom [Resource][Resource] implementation. Resources are
distributed objects that are associated with [nodes](#nodes) in the Copycat cluster. Each node can be associated
with a single resource, and each resource is backed by a replicated state machine managed by Copycat's underlying
[implementation of the Raft consensus protocol](#raft-consensus-algorithm).

Resources are created by simply passing a `Resource` class to one of Copycat's `create` methods:

```java
DistributedMap<String, String> map = copycat.create("/test-map", DistributedMap.class);
```

Copycat uses the provided `Class` to create an associated [StateMachine](#state-machines) on each server.
This allows users to create and integrate [custom resources](#custom-resources).

#### Persistence model

Copycat clients and servers communicate with each other through [sessions](#sessions). Each session represents
a persistent connection between a single client and a complete Copycat cluster. Sessions allow Copycat to associate
resource state changes with clients, and this information can often be used to manage state changes in terms of
sessions as well.

Some Copycat resources expose a configurable `PersistenceLevel` for resource state change operations. The
persistence level specifies whether a state change is associated directly with the client's `Session`.
Copycat exposes two persistence levels:
* `PersistenceLevel.PERSISTENT` - State changes persist across session changes
* `PersistenceLevel.EPHEMERAL` - State changes are associated directly with the session that created them

The `EPHEMERAL` persistence level allows resource state changes to be reflected only as long as the session
that created them remains alive. For instance, if a `DistributedMap` key is set with `PersistenceLevel.EPHEMERAL`,
the key will disappear from the map when the session that created it expires or is otherwise closed.

#### Consistency model

When performing operations on resources, Copycat separates the types of operations into two categories:
* *commands* - operations that alter the state of a resource
* *queries* - operations that query the state of a resource

The [Raft consensus algorithm](#raft-consensus-algorithm) on which Copycat is built guarantees linearizability for
*commands* in all cases. When a command is submitted to the cluster, the command will always be forwarded to the cluster
leader and replicated to a majority of servers before being applied to the resource's state machine and completed.

Alternatively, Copycat allows for optional trade-offs in the case of *queries*. These optimizations come at the expense
of consistency. When a query is submitted to the cluster, users can often specify the minimum consistency level of the
request by providing a `ConsistencyLevel` constant. The four minimum consistency levels available are:
* `ConsistencyLevel.LINEARIZABLE` - Provides guaranteed linearizability by forcing all reads to go through the leader and
  verifying leadership with a majority of the Raft cluster prior to the completion of all operations
* `ConsistencyLevel.LINEARIZABLE_LEASE` - Provides best-effort optimized linearizability by forcing all reads to go through the leader
  but allowing most queries to be executed without contacting a majority of the cluster so long as less than the
  election timeout has passed since the last time the leader communicated with a majority
* `ConsistencyLevel.SEQUENTIAL` - Provides sequential consistency by allowing clients to read from followers but ensuring that state
  does not go back in time from the perspective of any given session
* `ConsistencyLevel.SERIALIZABLE` - Provides serializable consistency by allowing clients to read from followers members without any
  additional checks

Overloaded methods with `ConsistencyLevel` parameters are provided throughout Copycat's resources wherever it makes sense.

## Distributed resources

Copycat provides a number of resource implementations for common distributed systems problems. Currently,
the provided resources are divided into three subsets that are represented as Maven submodules:

* [Distributed collections](#distributed-collections) - `DistributedSet`, `DistributedMap`, etc
* [Distributed atomic variables](#distributed-atomic-variables) - `DistributedAtomicValue`, etc
* [Distributed coordination tools](#distributed-coordination) - `DistributedLock`, `DistributedLeaderElection`, etc

### Distributed collections

The `copycat-collections` module provides a set of asynchronous, distributed collection-like [resources](#resources).
The resources provided by the collections module do not implement JDK collection interfaces because Copycat's
APIs are asynchronous, but their methods are equivalent to their blocking counterparts and so collection
resources can be easily wrapped in blocking collection interfaces.

If your project does not depend on `copycat-all`, you must add the `copycat-collections` dependency in order
to access the collection classes:

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-collections</artifactId>
  <version>0.6.0-SNAPSHOT</version>
</dependency>
```

#### DistributedSet

The [DistributedSet][DistributedSet] resources provides an asynchronous API similar to that of `java.util.Set`.

To create a `DistributedSet`, pass the class to `Copycat.create(String, Class)` or `Node.create(Class)`:

```java
copycat.<DistributedSet<String>>create("/test-set", DistributedSet.class).thenAccept(set -> {
  // Do something with the set
});
```

Once the set has been created, the methods closely mimic those of `java.util.Set`. `DistributedSet` returns
`CompletableFuture` for all methods:

```java
set.add("Hello world!").thenRun(() -> {
  set.contains("Hello world!").thenAccept(result -> {
    assert result;
  });
});
```

To block and wait for results instead, call `join()` or `get()` on the returned `CompletableFuture`s:

```java
set.add("Hello world!").join();
assert set.contains("Hello world!").get();
```

##### Expiring values

`DistributedSet` supports configurable TTLs for set values. To set a TTL on a value, simply pass a
`Duration` when adding a value to the set:

```java
set.add("Hello world!", Duration.ofSeconds(1)).thenAccept(succeeded -> {
  // If the add failed, the TTL will not have been set
  if (succeeded) {
    System.out.println("Value added with TTL");
  } else {
    System.out.println("Value add failed");
  }
});
```

Note that TTL timers are deterministically controlled by the cluster leader and are approximate representations
of wall clock time that *should not be relied upon for accuracy*.

##### Ephemeral values

In addition to supporting time-based state changes, `DistributedSet` also supports session-based changes via
a configurable [PersistenceMode](#persistence-mode). When a value is added to the set with `PersistenceMode.EPHEMERAL`,
the value will disappear once the session that created the value is expired or closed.

```java
// Add a value with EPHEMERAL persistence
set.add("Hello world!", PersistenceLevel.EPHEMERAL).thenRun(() -> {
  // Close the Copycat instance to force the value to be removed from the set
  copycat.close();
});
```

#### DistributedMap

The [DistributedMap][DistributedMap] resources provides an asynchronous API similar to that of `java.util.Map`.

To create a `DistributedMap`, pass the class to `Copycat.create(String, Class)` or `Node.create(Class)`:

```java
copycat.<DistributedMap<String, String>>create("/test-map", DistributedMap.class).thenAccept(map -> {
  // Do something with the map
});
```

Once the map has been created, the methods closely mimic those of `java.util.Map`. `DistributedMap` returns
`CompletableFuture` for all methods:

```java
map.put("foo", "Hello world!").thenRun(() -> {
  map.get("foo").thenAccept(result -> {
    assert result.equals("Hello world!");
  });
});
```

To block and wait for results instead, call `join()` or `get()` on the returned `CompletableFuture`s:

```java
map.put("foo", "Hello world!").join();
assert map.get("foo").get().equals("Hello world!");
```

##### Expiring keys

`DistributedMap` supports configurable TTLs for map keys. To set a TTL on a key, simply pass a
`Duration` when adding a key to the map:

```java
map.put("foo", "Hello world!", Duration.ofSeconds(1)).thenRun(() -> {
  System.out.println("Key added with TTL");
});
```

Note that TTL timers are deterministically controlled by the cluster leader and are approximate representations
of wall clock time that *should not be relied upon for accuracy*.

##### Ephemeral keys

In addition to supporting time-based state changes, `DistributedMap` also supports session-based changes via
a configurable [PersistenceMode](#persistence-mode). When a key is added to the map with `PersistenceMode.EPHEMERAL`,
the key will disappear once the session that created the key is expired or closed.

```java
// Add a key with EPHEMERAL persistence
map.put("foo", "Hello world!", PersistenceLevel.EPHEMERAL).thenRun(() -> {
  // Close the Copycat instance to force the key to be remove from the map
  copycat.close();
});
```

### Distributed atomic variables

The `copycat-atomic` module provides a set of distributed atomic variables modeled on Java's `java.util.concurrent.atomic`
package. The resources provided by the atomic module do not implement JDK atomic interfaces because Copycat's
APIs are asynchronous, but their methods are equivalent to their blocking counterparts and so atomic resources
can be easily wrapped in blocking interfaces.

If your project does not depend on `copycat-all`, you must add the `copycat-atomic` dependency in order
to access the atomic classes:

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-atomic</artifactId>
  <version>0.6.0-SNAPSHOT</version>
</dependency>
```

#### DistributedAtomicValue

The [DistributedAtomicValue][DistributedAtomicValue] resource provides an asynchronous API similar to that of
`java.util.concurrent.atomic.AtomicReference`.

To create a `DistributedAtomicValue`, pass the class to `Copycat.create(String, Class)` or `Node.create(Class)`:

```java
copycat.<DistributedAtomicValue<String>>create("/test-value", DistributedAtomicValue.class).thenAccept(value -> {
  // Do something with the value
});
```

Once the value has been created, the methods closely mimic those of `java.util.concurrent.atomic.AtomicReference`.
`DistributedAtomicValue` returns `CompletableFuture` for all methods:

```java
value.set("Hello world!").thenRun(() -> {
  value.get().thenAccept(result -> {
    assert result.equals("Hello world!");
  });
});
```

To block and wait for results instead, call `join()` or `get()` on the returned `CompletableFuture`s:

```java
value.set("Hello world!").join();
assert value.get().get().equals("Hello world!");
```

##### Expiring value

`DistributedAtomicValue` supports configurable TTLs for values. To set a TTL on the value, simply pass a
`Duration` when setting the value:

```java
value.set("Hello world!", Duration.ofSeconds(1)).thenRun(() -> {
  System.out.println("Value set with TTL of 1 second");
});
```

Note that TTL timers are deterministically controlled by the cluster leader and are approximate representations
of wall clock time that *should not be relied upon for accuracy*.

##### Ephemeral value

In addition to supporting time-based state changes, `DistributedAtomicValue` also supports session-based changes via
a configurable [PersistenceMode](#persistence-mode). When the value is set with `PersistenceMode.EPHEMERAL`,
the value will disappear once the session that created the value is expired or closed.

```java
// Set the value with EPHEMERAL persistence
value.set("Hello world!", PersistenceMode.EPHEMERAL).thenRun(() -> {
  // Close the Copycat instance to force the value to be unset
  copycat.close();
});
```

### Distributed coordination

The `copycat-coordination` module provides a set of distributed coordination tools. These tools are designed to
facilitate decision making and communication in a distributed system.

If your project does not depend on `copycat-all`, you must add the `copycat-coordination` dependency in order
to access the coordination classes:

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-coordination</artifactId>
  <version>0.6.0-SNAPSHOT</version>
</dependency>
```

#### DistributedLock

The [DistributedLock][DistributedLock] resources provides an asynchronous API similar to that of
`java.util.concurrent.locks.Lock`.

To create a `DistributedLock`, pass the class to `Copycat.create(String, Class)` or `Node.create(Class)`:

```java
copycat.create("/test-lock", DistributedLock.class).thenAccept(lock -> {
  // Do something with the lock
});
```

Once the lock has been created, the methods closely mimic those of `java.util.concurrent.locks.Lock`.
`DistributedLock` returns `CompletableFuture` for all methods:

```java
lock.lock().thenRun(() -> {
  // Do some stuff and then...
  lock.unlock();
});
```

To block and wait for the lock to be acquired instead, call `join()` or `get()` on the returned `CompletableFuture`s:

```java
lock.lock().join();

// Do some stuff

lock.unlock().join();
```

#### DistributedLeaderElection

The [DistributedLeaderElection][DistributedLeaderElection] resource provides an asynchronous API for coordinating
tasks among a set of clients.

[Leader election](https://en.wikipedia.org/wiki/Leader_election) is a pattern commonly used in distributed systems
to coordinate some task or access to a resource among a set of processes. Copycat's `DistributedLeaderElection`
handles the coordination of a leader and notifies processes when they become the leader.

To create a `DistributedLeaderElection`, pass the class to `Copycat.create(String, Class)` or `Node.create(Class)`:

```java
copycat.create("/test-election", DistributedLeaderElection.class).thenAccept(election -> {
  // Do something with the election
});
```

Once the election has been created, register a listener callback to be called when the calling node is elected
the leader:

```java
election.onElection(epoch -> {
  System.out.println("Elected leader!");
});
```

The registration of a listener via `onElection` is asynchronous. The resource will not become electable
until the `CompletableFuture` returned has been completed:

```java
election.onElection(epoch -> {
  System.out.println("Elected leader!");
}).thenRun(() -> {
  System.out.println("Awaiting election!");
});
```

When a session creates a new `DistributedLeaderElection` at the `/test-election` path, the session will be
queued to be elected. When a client/session disconnects from the Copycat cluster or times out, the next
session awaiting the leadership role will take over the leadership and the registered `onElection` listener
will be called.

The argument provided to the election listener is commonly known as an *epoch* (or in some cases a `term`
as in [Raft][Raft]). The epoch is a monotonically increasing, unique `long` that is representative of a
single election.

It is important to note that while from the Copycat cluster's perspective, only one client will hold the
leadership at any given point in time, the same may not be true for clients. It's possible that a client
can believe itself to be the leader even though its session has timed out and a new leader has been elected.
Users can guard against this scenario by verifying leadership with the `isLeader(long)` method prior to critical
operations in order to ensure consistency:

```java
election.onElection(epoch -> {
  // Verify that this node is still the leader
  election.isLeader(epoch).thenAccept(leader -> {
    if (leader) {
      System.out.println("Still the leader");
      // Do something important
    } else {
      System.out.println("Lost leadership!");
    }
  });
});
```

In the event that a `DistributedLeaderElection` wins an election and loses its leadership without the
node crashes, it's likely that the client's session expired due to a failure to communicate with the
cluster.

#### DistributedTopic

The [DistributedTopic][DistributedTopic] resource provides an asynchronous API for sending publish-subscribe
messages between clients. Messages sent via a `DistributedTopic` are linearized through the client's [Session][Session].
This means messages are guaranteed to be delivered exactly once and in the order in which they were sent to all sessions
that are active at the time the message is sent.

To create a `DistributedTopic`, pass the class to `Copycat.create(String, Class)` or `Node.create(Class)`:

```java
copycat.<DistributedTopic<String>>create("/test-topic", DistributedTopic.class).thenAccept(topic -> {
  // Send and receive some messages with the topic
});
```

Once the topic has been created, we can send and receive messages. To send messages to the topic,
use the `publish(T)` method:

```java
topic.publish("Hello world!");
```

To receive messages sent to the topic, register a topic listener using the `onMessage` method:

```java
topic.onMessage(message -> {
  assert message.equals("Hello world!");
});
```

When a message is sent to a topic, the message will be logged and replicated like any state change
via Copycat's underlying [Raft](#raft-consensus-algorithm) implementation. Once the message is stored
on a majority of servers, the message will be delivered to any client [sessions](#sessions) alive at
the time the message was sent.

### Custom resources

The Copycat API is designed to facilitate operating on arbitrary user-defined resources. When a custom resource is created
via `Copycat.create`, an associated state machine will be created on each Copycat server, and operations submitted by the
resource instance will be applied to the replicated state machine. In that sense, we can think of a `Resource` instance
as a client-side object and a `StateMachine` instance as the server-side representation of that object.

To define a new resource, simply extend the base `Resource` class:

```java
public class Value extends Resource {

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return ValueStateMachine.class;
  }

}
```

The `Resource` implementation must return a `StateMachine` class that will be configured to manage the resource's state.

```java
copycat.create(Value.class).thenAccept(value -> {
  System.out.println("Value resource created!");
});
```

When a resource is created via `Copycat.create(String, Class)` or `Node.create(Class)`, the `StateMachine` class returned
by the `Resource.stateMachine()` method will be constructed on each server in the cluster. Once the state machine has been
created on a majority of the servers, the resource will be constructed and the returned `CompletableFuture` completed.

Resource state changes are submitted to the Copycat cluster as [Command][Command] or [Query][Query] implementations.
See the documentation on Raft [commands](#commands) and [queries](#queries) for specific information regarding the
use cases and limitations of each type.

To submit an operation to the Copycat cluster on behalf of the resource, expose a method that forwards a `Command` or
`Query` to the cluster:

```java
public class Value<T> extends Resource {

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return ValueStateMachine.class;
  }

  /**
   * Returns the value.
   */
  public CompletableFuture<T> get() {
    return submit(new Get<>());
  }

  /**
   * Sets the value.
   */
  public CompletableFuture<Void> set(T value) {
    return submit(new Set<>(value));
  }

  /**
   * Get query.
   */
  private static class Get<T> implements Query<T> {
  }

  /**
   * Set command.
   */
  private static class Set<T> implements Command<T> {
    private Object value;

    private Set() {
    }

    private Set(Object value) {
      this.value = value;
    }
  }

  /**
   * Value state machine.
   */
  private static class ValueStateMachine extends StateMachine {
    private Object value;

    @Override
    protected void configure(StateMachineExecutor executor) {
      executor.register(Get.class, this::get);
    }

    /**
     * Gets the value.
     */
    private Object get(Commit<Get> commit) {
      return value;
    }

    /**
     * Sets the value.
     */
    private void set(Commit<Set> commit) {
      this.value = commit.operation().value;
    }
  }

}
```

*Important: See [Raft state machine documentation](#state-machines) for details on cleaning commits
from the log*

## I/O & Serialization

Copycat provides a custom I/O and serialization framework that it uses for all disk and network I/O.
The I/O framework is designed to provide an abstract API for reading and writing bytes on disk, in memory,
and over a network in a way that is easily interchangeable and reduces garbage collection and unnecessary
memory copies.

The I/O subproject consists of several essential components:
* [Buffers](#buffers) - A low-level buffer abstraction for reading/writing bytes in memory or on disk
* [Serialization](#serialization) - A low-level serialization framework built on the `Buffer` API
* [Storage](#storage) - A low-level ordered and indexed, self-cleaning `Log` designed for use in the [Raft consensus algorithm][Raft]
* [Transport](#transports) - A low-level generalization of asynchronous client-server messaging

### Buffers

Copycat provides a [Buffer][Buffer] abstraction that provides a common interface to both memory and disk. Currently,
four buffer types are provided:
* `HeapBuffer` - on-heap `byte[]` backed buffer
* `DirectBuffer` - off-heap `sun.misc.Unsafe` based buffer
* `MemoryMappedBuffer` - `MappedByteBuffer` backed buffer
* `FileBuffer` - `RandomAccessFile` backed buffer

The [Buffer][Buffer] interface implements `BufferInput` and `BufferOutput` which are functionally similar to Java's
`DataInput` and `DataOutput` respectively. Additionally, features of how bytes are managed are intentionally similar
to [ByteBuffer][ByteBuffer]. Copycat's buffers expose many of the same methods such as `position`, `limit`, `flip`,
and others. Additionally, buffers are allocated via a static `allocate` method similar to `ByteBuffer`:

```java
Buffer buffer = DirectBuffer.allocate(1024);
```

Buffers are dynamically allocated and allowed to grow over time, so users don't need to know the number of bytes they're
expecting to use when the buffer is created.

The `Buffer` API exposes a set of `read*` and `write*` methods for reading and writing bytes respectively:

```java
Buffer buffer = HeapBuffer.allocate(1024);
buffer.writeInt(1024)
  .writeUnsignedByte(255)
  .writeBoolean(true)
  .flip();

assert buffer.readInt() == 1024;
assert buffer.readUnsignedByte() == 255;
assert buffer.readBoolean();
```

See the [Buffer API documentation][Buffer] for more detailed usage information.

#### Bytes

All `Buffer` instances are backed by a `Bytes` instance which is a low-level API over a fixed number of bytes. In contrast
to `Buffer`, `Bytes` do not maintain internal pointers and are not dynamically resizeable.

`Bytes` can be allocated in the same way as buffers, using the respective `allocate` method:

```java
FileBytes bytes = FileBytes.allocate(new File("path/to/file"), 1024);
```

Additionally, bytes can be resized via the `resize` method:

```java
bytes.resize(2048);
```

When in-memory bytes are resized, the memory will be copied to a larger memory space via `Unsafe.copyMemory`. When disk
backed bytes are resized, disk space will be allocated by resizing the underlying file.

#### Buffer pools

All buffers can optionally be pooled and reference counted. Pooled buffers can be allocated via a `PooledAllocator`:

```java
BufferAllocator allocator = new PooledHeapAllocator();

Buffer buffer = allocator.allocate(1024);
```

Copycat tracks buffer references by implementing the `ReferenceCounted` interface. When pooled buffers are allocated,
their `ReferenceCounted.references` count will be `1`. To release the buffer back to the pool, the reference count must
be decremented back to `0`:

```java
// Release the reference to the buffer
buffer.release();
```

Alternatively, `Buffer` extends `AutoCloseable`, and buffers can be released back to the pool regardless of their
reference count by calling `Buffer.close`:

```java
// Release the buffer back to the pool
buffer.close();
```

### Serialization

Copycat provides an efficient custom serialization framework that's designed to operate on both disk and memory via a
common [Buffer](#buffers) abstraction.

#### Serializer

Copycat's serializer can be used by simply instantiating a [Serializer][Serializer] instance:

```java
// Create a new Serializer instance with an unpooled heap allocator
Serializer serializer = new Serializer(new UnpooledHeapAllocator());

// Register the Person class with a serialization ID of 1
serializer.register(Person.class, 1);
```

Objects are serialized and deserialized using the `writeObject` and `readObject` methods respectively:

```java
// Create a new Person object
Person person = new Person(1234, "Jordan", "Halterman");

// Write the Person object to a newly allocated buffer
Buffer buffer = serializer.writeObject(person);

// Flip the buffer for reading
buffer.flip();

// Read the Person object
Person result = serializer.readObject(buffer);
```

The `Serializer` class supports serialization and deserialization of `CopycatSerializable` types, types that have an associated
`Serializer`, and native Java `Serializable` and `Externalizable` types, with `Serializable` being the most inefficient
method of serialization.

Additionally, Copycat support copying objects by serializing and deserializing them. To copy an object, simply use the
`Serializer.copy` method:

```java
Person copy = serializer.copy(person);
```

All `Serializer` instance constructed by Copycat use `ServiceLoaderResolver`. Copycat registers internal
`CopycatSerializable` types via `META-INF/services/net.kuujo.copycat.io.serializer.CopycatSerializable`. To register additional
serializable types, create an additional `META-INF/services/net.kuujo.copycat.io.serializer.CopycatSerializable` file and list
serializable types in that file.

`META-INF/services/net.kuujo.copycat.io.serializer.CopycatSerializable`

```
com.mycompany.SerializableType1
com.mycompany.SerializableType2
```

Users should annotate all `CopycatSerializable` types with the `@SerializeWith` annotation and provide a serialization
ID for efficient serialization. Alley cat reserves serializable type IDs `128` through `255` and Copycat reserves
`256` through `512`.

#### Pooled object deserialization

Copycat's serialization framework integrates with [object pools](#buffer-pools) to support allocating pooled objects
during deserialization. When a `Serializer` instance is used to deserialize a type that implements `ReferenceCounted`,
Copycat will automatically create new objects from a `ReferencePool`:

```java
Serializer serializer = new Serializer();

// Person implements ReferenceCounted<Person>
Person person = serializer.readObject(buffer);

// ...do some stuff with Person...

// Release the Person reference back to Copycat's internal Person pool
person.close();
```

#### Serializable type resolution

Serializable types are resolved by a user-provided [SerializableTypeResolver][SerializableTypeResolver]. By default,
Copycat uses a combination of the 

Copycat always registers serializable types provided by [PrimitiveTypeResolver][PrimitiveTypeResolver] and
[JdkTypeResolver][JdkTypeResolver], including the following types:
* Primitive types
* Primitive wrappers
* Primitive arrays
* Primitive wrapper arrays
* `String`
* `Class`
* `BigInteger`
* `BigDecimal`
* `Date`
* `Calendar`
* `TimeZone`
* `Map`
* `List`
* `Set`

Additionally, Copycat's Raft implementation uses [ServiceLoaderResolver][ServiceLoaderResolver] to register
types registered via Java's `ServiceLoader`

Users can resolve custom serializers at runtime via `Serializer.resolve` methods or register specific types
via `Serializer.register` methods.

To register a serializable type with an `Serializer` instance, the type must generally meet one of the following conditions:
* Implement `CopycatSerializable`
* Implement `Externalizable`
* Provide a `Serializer` class
* Provide a `SerializerFactory`

```java
Serializer serializer = new Serializer();
serializer.register(Foo.class, FooSerializer.class);
serializer.register(Bar.class);
```

Additionally, Copycat supports serialization of `Serializable` and `Externalizable` types without registration, but this
mode of serialization is inefficient as it requires that Copycat serialize the full class name as well.

#### Registration identifiers

Types explicitly registered with a `Serializer` instance can provide a registration ID in lieu of serializing class names.
If given a serialization ID, Copycat will write the serializable type ID to the serialized `Buffer` instance of the class
name and use the ID to locate the serializable type upon deserializing the object. This means *it is critical that all
processes that register a serializable type use consistent identifiers.*

To register a serializable type ID, pass the `id` to the `register` method:

```java
Serializer serializer = new Serializer();
serializer.register(Foo.class, FooSerializer.class, 1);
serializer.register(Bar.class, 2);
```

Valid serialization IDs are between `0` and `65535`. However, Copycat reserves IDs `128` through `255` for internal use.
Attempts to register serializable types within the reserved range will result in an `IllegalArgumentException`.

#### CopycatSerializable

Instead of writing a custom `TypeSerializer`, serializable types can also implement the `CopycatSerializable` interface.
The `CopycatSerializable` interface is synonymous with Java's native `Serializable` interface. As with the `Serializer`
interface, `CopycatSerializable` exposes two methods which receive both a [Buffer](#buffers) and a `Serializer`:

```java
public class Foo implements CopycatSerializable {
  private int bar;
  private Baz baz;

  public Foo() {
  }

  public Foo(int bar, Baz baz) {
    this.bar = bar;
    this.baz = baz;
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeInt(bar);
    serializer.writeObject(baz);
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    bar = buffer.readInt();
    baz = serializer.readObject(buffer);
  }
}
```

For the most efficient serialization, it is essential that you associate a serializable type `id` with all serializable
types. Type IDs can be provided during type registration or by implementing the `@SerializeWith` annotation:

```java
@SerializeWith(id=1)
public class Foo implements CopycatSerializable {
  ...

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeInt(bar);
    serializer.writeObject(baz);
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    bar = buffer.readInt();
    baz = serializer.readObject(buffer);
  }
}
```

For classes annotated with `@SerializeWith`, the ID will automatically be retrieved during registration:

```java
Serializer serializer = new Serializer();
serializer.register(Foo.class);
```

#### TypeSerializer

At the core of the serialization framework is the [TypeSerializer][TypeSerializer]. The `TypeSerializer` is a simple
interface that exposes two methods for serializing and deserializing objects of a specific type respectively. That is,
serializers are responsible for serializing objects of other types, and not themselves. Copycat provides this separate
serialization interface in order to allow users to create custom serializers for types that couldn't otherwise be
serialized by Copycat.

The `TypeSerializer` interface consists of two methods:

```java
public class FooSerializer implements TypeSerializer<Foo> {

  @Override
  public void write(Foo foo, BufferWriter writer, Serializer serializer) {
    writer.writeInt(foo.getBar());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Foo read(Class<Foo> type, BufferReader reader, Serializer serializer) {
    Foo foo = new Foo();
    foo.setBar(reader.readInt());
  }
}
```

To serialize and deserialize an object, we simply write to and read from the passed in `BufferWriter` or `BufferReader`
instance respectively. In addition to the reader/writer, the `Serializer` that is serializing or deserializing the
instance is also passed in. This allows the serializer to serialize or deserialize subtypes as well:

```java
public class FooSerializer implements TypeSerializer<Foo> {

  @Override
  public void write(Foo foo, BufferWriter writer, Serializer serializer) {
    writer.writeInt(foo.getBar());
    Baz baz = foo.getBaz();
    serializer.writeObject(baz, writer);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Foo read(Class<Foo> type, BufferReader reader, Serializer serializer) {
    Foo foo = new Foo();
    foo.setBar(reader.readInt());
    foo.setBaz(serializer.readObject(reader));
  }
}
```

Copycat comes with a number of native `TypeSerializer` implementations, for instance `ListSerializer`:

```java
public class ListSerializer implements TypeSerializer<List> {

  @Override
  public void write(List object, BufferWriter writer, Serializer serializer) {
    writer.writeUnsignedShort(object.size());
    for (Object value : object) {
      serializer.writeObject(value, writer);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List read(Class<List> type, BufferReader reader, Serializer serializer) {
    int size = reader.readUnsignedShort();
    List object = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      object.add(serializer.readObject(reader));
    }
    return object;
  }

}
```

### Storage

The [Storage][Storage] API provides an interface to a low-level ordered and index self-cleaning log
designed for use in the [Raft consensus algorithm](#raft-consensus-algorithm). Each server in a Copycat
cluster writes state changes to disk via the [Log][Log]. Logs are built on top of Copycat's [Buffer](#buffers)
abstraction, so the backing store can easily be switched between memory and disk.

When constructing a `RaftServer` or `CopycatServer`, users must provide the server with a `Storage`
instance which controls the underlying `Log`. `Storage` objects are built via the storage [Builder](#builders):

```java
Storage storage = Storage.builder()
  .withDirectory("logs")
  .withStorageLevel(StorageLevel.DISK)
  .build();
```

#### Log

*Note: Much of the following is relevant only to Copycat internals*

Underlying the [Storage][Storage] API is the [Log][Log].

```java
Log log = storage.open();
```

The `Log` is an ordered and indexed list of entries stored in memory or on disk. Logs consist of a set of segments.
Each segment represents range of entries in the overall log. Each segment is backed by two [buffers](#buffers), a
`HeapBuffer` and a `FileBuffer`; this allows entries to be written *either* to disk or memory. Entries are serialized
to disk using Copycat's [serialization framework](#serialization).

Entries can only be appended to the log:

```java
try (MyEntry entry = log.create(MyEntry.class)) {
  entry.setFoo("foo");
  entry.setBar(1);
  entry.setPersistenceLevel(PersistenceLevel.DISK);
  log.append(entry);
}
```

The `PersistenceLevel` controls how entries are stored. Entries appended with `PersistenceLevel.DISK` will be written
to disk via a `FileBuffer`, and entries with `PersistenceLevel.MEMORY` will be stored in an in-memory buffer. In the
event of a failure or other closure of the log, `PersistenceLevel.DISK` entries will persist, while `PersistenceLevel.MEMORY`
entries will be lost, so *be careful*!

Segment memory and disk buffers are each backed by an offset index. The offset index is responsible for tracking
the indexes and positions of entries in the segment. In order to preserve disk/memory space, the index stores entry
indices as offsets relative to the beginning of each segment. Additionally, each segment is limited to a maximum
size of `Integer.MAX_VALUE` so that the position of an entry cannot exceed 4 bytes. This means each entry in the
index consumes only 8 bytes - 4 for the offset and 4 for the position.

Offset indexes are also responsible for tracking entries that have been [cleaned from the segment](#log-cleaning).
When entries are cleaned from the log, a flag is set in the owning segment's offset index to indicate that the
entry is awaiting compaction. Clean flags are stored in a bit set in memory, so each segment consumes at least
`[num entries] / 8` bytes of memory.

Entries in the log are always keyed by an `index` - a monotonically increasing 64-bit number. But because of the
nature of [log cleaning](#log-cleaning) - allowing entries to arbitrarily be removed from the log - the log and
its segments are designed to allow entries to be missing *at any point in the log*. Over time, it is expected that
entries will be cleaned and compacted out of the log. The log and segments always store entries in as compact a
form as possible. Offset indexes contain only entries that have been physically written to the segment, and indexes
are searched with a binary search algorithm during reads.

#### Log cleaning

The most critical component of Copycat's [Log][Log] design relates to log cleaning. Cleaning is the process of
removing arbitrary entries from the log over time. Copycat's `Log` is designed to facilitate storing operations
on a state machine. Over time, as state machine operations become irrelevant to a state machine's state, they
can be marked for deletion from the log by the `clean(index)` method.

When an entry is `clean`ed from the log, the entry is internally marked for deletion from the log. Thereafter,
the entry will no longer be accessible via the `Log` interface. Internally, the log sets an in-memory bit indicating
that the entry at the given index is awaiting compaction.

Note, though, that calling `clean` does not mean that an entry will be removed from disk or memory. Entries are
only removed once the log rolls over to a new `Segment` or the user explicitly `clean`s the log:

```java
log.cleaner().clean();
```

When the log is cleaned, a background thread will evaluate the log's segments to determine whether they need to
be compacted. Currently, segments are compacted based on two factors:
* The number of entries that have been `clean`ed from the segment
* The number of times the segment has been previously cleaned

For a segment that has not yet been cleaned, cleaning will take place only once 50% of the entries in the segment
have been `clean`ed. The second time the segment is cleaned, 25% of its entries must have been `clean`ed, and so
forth.

Log cleaning works by simply creating a new segment at the start of the segment being cleaned and iterating over
the entries in the segment, rewriting live entries from the old segment to the new segment, and throwing out
entries that have been `clean`ed:

![Combining segments](http://s12.postimg.org/jhdthtpct/Combined_Segment_Compaction_New_Page_1.png)

This graphic depicts the cleaning process. As entries are appended to the log, some older entries are marked
for cleaning (the grey boxes). During the log cleaning process, a background thread iterates through the
segment being cleaned (the bold boxes) and discards entries that have been `clean`ed (the bold white boxes).
In the event that two neighboring segments have been compacted small enough to form a single segment, they
will be combined into one segment (the last row). This ensures that the number of open files remains more or
less constant as entries are cleaned from the log.

### Transports

The [Transport][Transport] API provides an interface that generalizes the concept of asynchronous
client-server messaging. `Transport` objects control the communication between all clients and servers
throughout a Copycat cluster. Therefore, it is essential that all nodes in a cluster use the same transport.

The [NettyTransport][NettyTransport] is a TCP-based transport built on [Netty](http://netty.io/) 4.

```java
Transport transport = new NettyTransport();
```

For test cases, Copycat provides the [LocalTransport][LocalTransport] which mimics the behavior of a
network based transport via threads and executors.

## Raft consensus algorithm

Copycat is built on a standalone, feature-complete implementation of the [Raft consensus algorithm][Raft].
The Raft implementation consists of three Maven submodules:

#### copycat-protocol

The `copycat-protocol` submodule provides base interfaces and classes that are shared between both
the [client](#copycat-client) and [server](#copycat-server) modules. The most notable components of
the protocol submodule are [commands][Command] and [queries][Query] with which the client
communicates state machine operations, and [sessions][Session] through which clients and servers
communicate.

#### copycat-server

The `copycat-server` submodule is a standalone [Raft][Raft] server implementation. The server provides
a feature-complete implementation of the [Raft consensus algorithm][Raft], including dynamic cluster
membership changes and log compaction.

The primary interface to the `copycat-server` module is [RaftServer][RaftServer].

#### copycat-client

The `copycat-client` submodule provides a [RaftClient][RaftClient] interface for submitting [commands][Command]
and [queries][Query] to a cluster of [RaftServer][RaftServer]s. The client implementation includes full support
for linearizable commands via [sessions][Session].

### RaftServer

The [RaftServer][RaftServer] class is a feature complete implementation of the [Raft consensus algorithm][Raft].
`RaftServer` underlies all distributed resources supports by Copycat's high-level APIs.

The `RaftServer` class is provided in the `copycat-server` module:

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-server</artifactId>
  <version>0.6.0-SNAPSHOT</version>
</dependency>
```

Each `RaftServer` consists of three essential components:
* [Transport](#transports) - Used to communicate with clients and other Raft servers
* [Storage](#storage) - Used to persist [commands](#commands) to memory or disk
* [StateMachine](#state-machines) - Represents state resulting from [commands](#commands) logged and replicated via Raft

To create a Raft server, use the server [Builder](#builders):

```java
RaftServer server = RaftServer.builder()
  .withMemberId(1)
  .withMembers(members)
  .withTransport(new NettyTransport())
  .withStorage(Storage.builder()
    .withStorageLevel(StorageLevel.MEMORY)
    .build())
  .withStateMachine(new MyStateMachine())
  .build();
```

Once the server has been created, call `open()` to start the server:

```java
server.open().thenRun(() -> System.out.println("Server started successfully!"));
```

The returned `CompletableFuture` will be completed once the server has connected to other members of
the cluster and, critically, discovered the cluster leader. See the [server lifecycle](#server-lifecycle)
for more information on how the server joins the cluster.

### Server lifecycle

Copycat's Raft implementation supports dynamic membership changes designed to allow servers to arbitrarily join and leave the
cluster. When a `RaftServer` is configured, the `Members` list provided in the server configuration specifies some number of
servers to join to form a cluster. When the server is started, the server begins a series of steps to either join an existing
Raft cluster or start a new cluster:

* When the server starts, transition to a *join* state and attempt to join the cluster by sending a *join* request to each known
  `Member` of the cluster
* If, after an election timeout, the server has failed to receive a response to a *join* requests from any `Member` of the cluster,
  assume that the cluster doesn't exist and transition into the *follower* state
* Once a leader has been elected or otherwise discovered, complete the startup

When a member *joins* the cluster, a *join* request will ultimately be received by the cluster's leader. The leader will log and
replicate the joining member's configuration. Once the joined member's configuration has been persisted on a majority of the cluster,
the joining member will be notified of the membership change and transition to the *passive* state. While in the *passive* state,
the joining member cannot participate in votes but does receive *append* requests from the cluster leader. Once the leader has
determined that the joining member's log has caught up to its own (the joining node's log has the last committed entry at any given
point in time), the member is promoted to a full member via another replicated configuration change.

Once a node has fully joined the Raft cluster, in the event of a failure the quorum size will not change. To leave the cluster,
the `close()` method must be called on a `CopycatServer` instance. When `close()` is called, the member will submit a *leave* request
to the leader. Once the leaving member's configuration has been removed from the cluster and the new configuration replicated and
committed, the server will complete the close.

### Commands

Commands are operations that modify the state machine state. When a command operation is submitted to the Copycat cluster,
the command is logged to disk or memory (depending on the [Storage](#storage) configuration) and replicated via the Raft consensus
protocol. Once the command has been stored on a majority cluster members, it will be applied to the server-side
[StateMachine](#state-machines) and the output will be returned to the client.

Commands are defined by implementing the `Command` interface:

```java
public class Set<T> implements Command<T> {
  private final String value;

  public Set(String value) {
    this.value = value;
  }

  /**
   * The value to set.
   */
  public String value() {
    return value;
  }
}
```

The [Command][Command] interface extends [Operation][Operation] which is `Serializable` and can be sent over the wire with no
additional configuration. However, for the best performance users should implement [CopycatSerializable][CopycatSerializable]
or register a [TypeSerializer][TypeSerializer] for the type. This will reduce the size of the serialized object and allow
Copycat's [Serializer](#serializer) to optimize class loading internally during deserialization.

### Command persistence

As mentioned, commands are operations that modify the state machine state, but there is one caveat to that rule. Copycat
provides the option for commands to be stored in-memory only via the `PersistenceLevel` option. By default, all commands
are persisted with `PersistenceLevel.DISK` so that they are not lost in the event of a failure. But Copycat supports
`PersistenceLevel.MEMORY` commands in order to facilitate replicating events to all servers in the cluster.

```java
public class Publish<T> implements Command {

  @Override
  public PersistenceLevel persistence() {
    return PersistenceLevel.MEMORY;
  }

}
```

`PersistenceLevel.MEMORY` commands should *never alter state machine state* since they can be lost during a failure
and therefore result in non-deterministic state (two nodes with divergent state). Instead, they should be used for
one-time events that need to reach all [sessions](#sessions). Each client's [Session][Session] is associated with
a random Raft server (follower or leader), and so for an event to reach all sessions it must reach all servers.
Committing a `PersistenceLevel.MEMORY` command allows it to be replicated to all servers without the additional
I/O overhead.

### Queries

In contrast to commands which perform state change operations, queries are read-only operations which do not modify the
server-side state machine's state. Because read operations do not modify the state machine state, Copycat can optimize
queries according to read from certain nodes according to the configuration and [may not require contacting a majority
of the cluster in order to maintain consistency](#query-consistency). This means queries can significantly reduce disk and
network I/O depending on the query configuration, so it is strongly recommended that all read-only operations be implemented as queries.

To create a query, simply implement the [Query][Query] interface:

```java
public class Get<T> implements Query {
}
```

As with [Command][Command], [Query][Query] extends the base [Operation][Operation] interface which is `Serializable`. However,
for the best performance users should implement [CopycatSerializable][CopycatSerializable] or register a
[TypeSerializer][TypeSerializer] for the type.

#### Query consistency

By default, [queries](#queries) submitted to the Copycat cluster are guaranteed to be linearizable. Linearizable queries are
forwarded to the leader where the leader verifies its leadership with a majority of the cluster before responding to the request.
However, this pattern can be inefficient for applications with less strict read consistency requirements. In those cases, Copycat
allows [Query][Query] implementations to specify a `ConsistencyLevel` to control how queries are handled by the cluster.

To configure the consistency level for a `Query`, simply override the default `consistency()` getter:

```java
public class Get<T> implements Query {

  @Override
  public ConsistencyLevel consistency() {
    return Consistency.SEQUENTIAL;
  }

}
```

The consistency level returned by the overridden `consistency()` method amounts to a *minimum consistency requirement*.
In many cases, a `SEQUENTIAL` consistency level can actually result in `LINEARIZABLE` consistency depending the server
to which a client submits queries.

Copycat provides four consistency levels:
* `ConsistencyLevel.LINEARIZABLE` - Provides guaranteed linearizability by forcing all reads to go through the leader and
  verifying leadership with a majority of the Raft cluster prior to the completion of all operations
* `ConsistencyLevel.LINEARIZABLE_LEASE` - Provides best-effort optimized linearizability by forcing all reads to go through the
  leader but allowing most queries to be executed without contacting a majority of the cluster so long as less than the
  election timeout has passed since the last time the leader communicated with a majority
* `ConsistencyLevel.SEQUENTIAL` - Provides sequential consistency by allowing clients to read from followers but ensuring that
  state does not go back in time from the perspective of any given session
* `ConsistencyLevel.SERIALIZABLE` - Provides serializable consistency by allowing clients to read from followers members without
  any additional checks

### State machines

State machines are the server-side representation of state based on a series of [commands](#commands) and [queries](#queries)
submitted to the Raft cluster.

**All state machines must be deterministic**

Non-deterministic state machines will break the guarantees of the Raft consensus algorithm. Each [server](#raftserver) in the
cluster must have *the same state machine*. When a command is submitted to the cluster, the command will be forwarded to the leader,
logged to disk or memory, and replicated to a majority of the cluster before being applied to the state machine, and the return
value for a given command or query is returned to the requesting client.

State machines are created by extending the base `StateMachine` class and overriding the `configure(StateMachineExecutor)` method:

```java
public class MyStateMachine extends StateMachine {

  @Override
  protected void configure(StateMachineExecutor executor) {
  
  }

}
```

Internally, state machines are backed by a series of entries in an underlying [log](#log). In the event of a crash and
recovery, `PersistenceLevel.DISK` commands in the log will be replayed to the state machine, and `PersistenceLevel.MEMORY`
commands and queries will be lost. For this reason, `PersistenceLevel.MEMORY` commands and queries should never alter
state machine state since the removal of such commands/queries would result in non-deterministic state machine state
(state on two different nodes would differ based on the commands in their logs).

#### StateMachineExecutor

The `StateMachineExecutor` is a special [Context](#contexts) implemntation that is responsible for applying [commands](#commands)
and [queries](#queries) to the state machine. Operations are handled by registering callbacks on the provided `StateMachineExecutor`
in the `configure` method:

```java
@Override
protected void configure(StateMachineExecutor executor) {
  executor.register(SetCommand.class, this::set);
  executor.register(GetQuery.class, this::get);
}
```

In addition to registering operation callbacks, the `StateMachineExecutor` also facilitates deterministic scheduling based on
the Raft replicated log.

```java
executor.schedule(() -> System.out.println("Every second"), Duration.ofSeconds(1), Duration.ofSeconds(1));
```

Because of the complexities of coordinating distributed systems, time does not move at the same rate on all servers in the cluster.
What is essential, though, is that time-based callbacks be executed at the same point in the Raft log on all nodes. In order to
accomplish this, the leader writes an approximate `Instant` to the replicated log for each command. When a command is applied to the
state machine, the command's timestamp is used to invoke any outstanding scheduled callbacks. This means the granularity of
scheduled callbacks is limited by the minimum time between commands submitted to the cluster, including session register and
keep-alive requests. Thus, users should not rely on `StateMachineExecutor` scheduling for accuracy.

#### StateMachineContext

The `StateMachineContext` provides a view of the local server's state at the time a [command](#command) or [query](#queries)
is applied to the state machine. Users can use the context to access, for instance, the list of `Session`s currently registered
in the cluster.

To get the context, call the protected `context()` getter from inside the state machine:

```java
for (Session session : context().sessions()) {
  session.publish("Hello world!");
}
```

#### Commits

As [commands](#commands) and [queries](#queries) are logged and replicated through the Raft cluster, they gain some metadata
that is not present in the original operation. By the time operations are applied to the state machine, they've gained valuable
information that is exposed in the [Commit][Commit] wrapper class:

* `Commit.index()` - The sequential index of the commit in the underlying `Log`. The index is guaranteed to increase monotonically
  as commands are applied to the state machine. However, because [queries](#queries) are not logged, they may duplicate the indices
  of commands.
* `Commit.time()` - The approximate `Instant` at which the commit was logged by the leader through which it was committed. The commit
  time is guaranteed never to decrease.
* `Commit.session()` - The [Session](#sessions) that submitted the operation to the cluster. This can be used to send events back
  to the client.
* `Commit.operation()` - The operation that was committed.

```java
protected Object get(Commit<GetQuery> commit) {
  return map.get(commit.operation().key());
}
```

#### Sessions

Sessions represent connections between the `RaftClient` and `RaftServer` through which `StateMachine` implementations can send events
to clients. Copycat's Raft clients are required to only connect to a single `RaftServer` at any given time. However, Copycat exposes
the same `Session` objects to state machines on all servers. State machines can use the `Session` to `publish` events to the client.

```java
protected Object get(Commit<GetQuery> commit) {
  commit.session().publish("got");
  return map.get(commit.operation().key());
}
```

When an event is published to a client, only the server to which the client is connected will send the event to the client, thus
ensuring that the client only receives one event for the entire cluster. If the client is disconnected when a message is published,
the message will be queued in memory and delivered to the session by the next server to which the session's client connects. As
events are received by the client, the server removes received events from memory. Once the session is expired or closed, all
servers remove the session and its events from memory.

#### Commit cleaning

As commands are submitted to the cluster and applied to the Raft state machine, the underlying [log](#log) grows.
Without some mechanism to reduce the size of the log, the log would grow without bound and ultimately servers would
run out of disk space. Raft suggests a few different approaches of handling log compaction. Copycat uses the
[log cleaning](#log-cleaning) approach.

`Commit` objects are backed by entries in Copycat's replicated log. When a `Commit` is no longer neede dby the
`StateMachine`, the state machine should clean the commit from Copycat's log by calling the `clean()` method:

```java
protected void remove(Commit<RemoveCommand> commit) {
  map.remove(commit.operation().key());
  commit.clean();
}
```

Internally, the `clean()` call will be proxied to Copycat's underlying log:

```java
log.clean(commit.index());
```

As commits are cleaned by the state machine, entries in the underlying log will be marked for deletion. *Note
that it is not safe to assume that once a commit is cleaned it is permanently removed from the log*. Cleaning
an entry only *marks* it for deletion, and the entry won't actually be removed from the log until a background
thread cleans the relevant log segment. This means in the event of a crash-recovery and replay of the log,
a previously `clean`ed commit may still exists. For this reason, if a commit is dependent on a prior commit,
state machines should only `clean` those commits if no prior related commits have been seen. (More on this
later)

Once the underlying `Log` has grown large enough, and once enough commits have been `clean`ed from the log,
a pool of background threads will carry out their task to rewrite segments of the log to remove commits
(entries) for which `clean()` has been called:

![Raft cleaning](http://s21.postimg.org/fvlvlg9lz/Raft_Compaction_New_Page_3.png)

### RaftClient

The [RaftClient][RaftClient] provides an interface for submitting [commands](#commands) and [queries](#queries) to a cluster
of [Raft servers](#raftserver).

To create a client, you must supply the client [Builder](#builders) with a set of `Members` to which to connect.

```java
Members members = Members.builder()
  .addMember(Member.builder()
    .withId(1)
    .withHost("123.456.789.1")
    .withPort(5555)
    .build())
  .addMember(Member.builder()
    .withId(2)
    .withHost("123.456.789.2")
    .withPort(5555)
    .build())
  .addMember(Member.builder()
    .withId(3)
    .withHost("123.456.789.3")
    .withPort(5555)
    .build())
  .build();
```

The provided `Members` do not have to be representative of the full Copycat cluster, but they do have to provide at
least one correct server to which the client can connect. In other words, the client must be able to communicate with
at least one `CopycatServer` that is the leader or can communicate with the leader, and a majority of the cluster
must be able to communicate with one another in order for the client to register a new [Session](#session).

```java
RaftClient client = RaftClient.builder()
  .withTransport(new NettyTransport())
  .withMembers(members)
  .build();
```

Once a `RaftClient` has been created, connect to the cluster by calling `open()` on the client:

```java
client.open().thenRun(() -> System.out.println("Successfully connected to the cluster!"));
```

When the client is opened, it will connect to a random server and attempt to register its session. If session registration fails,
the client will continue to attempt registration via random servers until all servers have been tried. If the session cannot be
registered, the `CompletableFuture` returned by `open()` will fail.

#### Session

Once the client's session has been registered, the `Session` object can be accessed via `RaftClient.session()`.

The client will remain connected to the server through which the session was registered for as long as possible. If the server
fails, the client can reconnect to another random server and maintain its open session.

The `Session` object can be used to receive events `publish`ed by the server's `StateMachine`. To register a session event listener,
use the `onReceive` method:

```java
client.session().onReceive(message -> System.out.println("Received " + message));
```

When events are sent from a server state machine to a client via the `Session` object, only the server to which the client is
connected will send the event. Copycat servers guarantee that state machine events will be received by the client session in the
order in which they're sent even if the client switches servers.

## Miscellaneous

The following documentation explains the usage of various miscellaneous APIs provided by the `copycat-common` module.

### Builders

Throughout the project, Copycat often uses the [builder pattern](https://en.wikipedia.org/wiki/Builder_pattern) in lieu
of constructors to provide users with a fluent interface for configuring complex objects. Builders are implementations
of the [Builder][Builder] interface and in most cases are nested within the type they build. For instance, the
`CopycatClient.Builder` builds a [CopycatClient][CopycatClient] instance. Additionally, builders usually have an associated
static `builder()` method that can be used to retrieve a builder:

```java
CopycatClient.Builder builder = CopycatClient.builder();
```

The reasoning behind using a static factory method for builders is in order to transparently support recycling
builders. In some cases, builders are used to configure short-lived objects such as [commands][Command] and
[Queries][query]. In those cases, rather than constructing a new `Builder` for each instance (thus resulting
in two objects being created for one), Copycat recycles builders via the `builder()` factory method.

### Listeners

Copycat largely provides its API for asynchronous callbacks via Java 8's [CompletableFuture][CompletableFuture].
But in some cases, users need to register to receive events that are invoked by Copycat internally. For those
cases, Copycat provides a [Listener][Listener] to help manage event listeners.

Listeners work by first registering a `Consumer` for an event:

```java
DistributedTopic<String> topic = copycat.create("/topic", DistributedTopic.class).get();

Listener<String> listener = topic.onMessage(message -> System.out.println("Received " + message)).get();
```

The `Listener` acts as a registration for the user-provided `Consumer` and allows the user to unregister
the listener simply by calling the `close()` method:

```java
// Stop listening for messages.
listener.close();
```

### Contexts

[Contexts][Context] are used by Copycat internally to control thread scheduling and execution. At a
low level, `Context` implementations wrap single-thread or thread-pool [Executors][Executor]. All
threads within a running Copycat cluster have an associated `Context`. The `Context` holds
thread-unsafe objects such as a `Serializer` clone per thread.

### [User Manual](#user-manual)

## [Javadoc][Javadoc]

[Javadoc]: http://kuujo.github.io/copycat/api/0.6.0/
[Executor]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/Executor.html
[CompletableFuture]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html
[collections]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/collections.html
[atomic]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/atomic.html
[coordination]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/coordination.html
[copycat]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat.html
[raft]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/raft.html
[protocol]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/raft/protocol.html
[io]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io.html
[serializer]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/serializer.html
[transport]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/transport.html
[storage]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/storage.html
[utilities]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/util.html
[Raft]: https://raftconsensus.github.io/
[Copycat]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/Copycat.html
[CopycatServer]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/CopycatServer.html
[CopycatClient]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/CopycatClient.html
[Resource]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/Resource.html
[Transport]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/transport/Transport.html
[LocalTransport]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/transport/LocalTransport.html
[NettyTransport]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/transport/NettyTransport.html
[Storage]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/storage/Storage.html
[Log]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/storage/Log.html
[Buffer]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/Buffer.html
[BufferReader]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/BufferReader.html
[BufferWriter]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/BufferWriter.html
[Serializer]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/serializer/Serializer.html
[CopycatSerializable]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/serializer/CopycatSerializable.html
[TypeSerializer]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/serializer/TypeSerializer.html
[SerializableTypeResolver]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/serializer/SerializableTypeResolver.html
[PrimitiveTypeResolver]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/serializer/SerializableTypeResolver.html
[JdkTypeResolver]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/serializer/SerializableTypeResolver.html
[ServiceLoaderTypeResolver]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/serializer/ServiceLoaderTypeResolver.html
[RaftServer]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/raft/RaftServer.html
[RaftClient]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/raft/RaftClient.html
[Session]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/raft/session/Session.html
[Operation]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/raft/protocol/Operation.html
[Command]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/raft/protocol/Command.html
[Query]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/raft/protocol/Query.html
[Commit]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/raft/protocol/Commit.html
[ConsistencyLevel]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/raft/protocol/ConsistencyLevel.html
[DistributedAtomicValue]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/atomic/DistributedAtomicValue.html
[DistributedSet]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/collections/DistributedSet.html
[DistributedMap]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/collections/DistributedMap.html
[DistributedLock]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/coordination/DistributedLock.html
[DistributedLeaderElection]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/coordination/DistributedLeaderElection.html
[DistributedTopic]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/coordination/DistributedTopic.html
[Builder]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/util/Builder.html
[Listener]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/util/Listener.html
[Context]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/util/concurrent/Context.html
