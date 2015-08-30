Copycat
=======

**Persistent - Consistent - Fault-tolerant - Database - Coordination - Framework**

[![Build Status](https://travis-ci.org/kuujo/copycat.png)](https://travis-ci.org/kuujo/copycat)

#### [User Manual](#user-manual-1)
#### [Raft Consensus Algorithm](#raft-consensus-algorithm-1)
#### [Javadocs][Javadoc]
#### [Google Group](https://groups.google.com/forum/#!forum/copycat)

Copycat is both a low-level implementation of the [Raft consensus algorithm][Raft] and a high-level distributed
coordination framework that combines the consistency of [ZooKeeper](https://zookeeper.apache.org/) with the
usability of [Hazelcast](http://hazelcast.org/) to provide tools for managing and coordinating stateful resources
in a distributed system. Its strongly consistent, fault-tolerant data store is designed for such use cases as
configuration management, service discovery, and distributed synchronization.

Copycat exposes a set of high level APIs with tools to solve a variety of distributed systems problems including:
* [Distributed coordination tools](#distributed-coordination)
* [Distributed collections](#distributed-collections)
* [Distributed atomic variables](#distributed-atomic-variables)

Additionally, Copycat is built on a series of low-level libraries that form its consensus algorithm. Users can extend
Copycat to build custom managed replicated state machines. All base libraries are provided as standalone modules wherever
possible, so users can use many of the following components of the Copycat project independent of higher level libraries:
* A low-level [I/O & serialization framework](#io--serialization)
* A generalization of [asynchronous client-server messaging](#transports) with a [Netty implementation][nettytransport]
* A fast, persistent, cleanable [commit log](#storage) designed for use with the [Raft consensus algorithm][Raft]
* A feature-complete [implementation of the Raft consensus algorithm](#raft-consensus-algorithm)
* A lightweight [Raft client](#raftclient) including support for linearizable operations via [sessions](#sessions)

**Copycat is still undergoing heavy development and testing and is therefore not recommended for production!**

[Jepsen](https://github.com/aphyr/jepsen) tests are [currently being developed](http://github.com/jhalterman/copycat-jepsen)
to verify the stability of Copycat in an unreliable distributed environment. There is still work to be done, and Copycat
will not be fully released until significant testing is done both via normal testing frameworks and Jepsen. In the meantime,
Copycat snapshots will be pushed, and a beta release of Copycat is expected within the coming weeks. Follow the project for
updates!

*Copycat requires Java 8*

Getting Started
===============

The high-level [Copycat API](#the-copycat-api) is a single interface that aids in managing stateful resources
(e.g. maps, sets, locks, leader elections) in a distributed system. The `Copycat` API is heavily influenced by
[Hazelcast's](http://hazelcast.org/) API.

The one critical method in Copycat's high-level API is the `create` method. The `create` method gets or creates a
user-defined distributed resource.

```java
copycat.create("test-lock", DistributedLock.class).thenAccept(lock -> {
  // Do stuff...
});
```

Copycat's API is fully asynchronous and relies heavily on Java 8's [CompletableFuture][CompletableFuture].

#### DistributedMap

**Synchronous API**

```java
DistributedMap<String, String> map = copycat.create("test-map", DistributedMap.class).get();

map.put("key", "value").join();

String value = map.get("key").get();

if (value.equals("value")) {
  map.putIfAbsent("otherkey", "othervalue", Duration.ofSeconds(1)).join();
}
```

**Asynchronous API**

```java
DistributedMap<String, String> map = copycat.create("test-map", DistributedMap.class).thenAccept(map -> {
  map.put("key", "value").thenRun(() -> {
    map.get("key").thenAccept(value -> {
      if (value.equals("value")) {
        // Set a key with a TTL
        map.putIfAbsent("otherkey", "othervalue", Duration.ofSeconds(1)).get();
      }
    });
  });
});
```

#### DistributedSet

**Synchronous API**

```java
DistributedSet<String> set = copycat.create("test-set", DistributedSet.class).get();

set.add("value").join();

if (set.contains("value").get()) {
  set.add("othervalue", Duration.ofSeconds()).join();
}
```

**Asynchronous API**

```java
DistributedSet<String> set = copycat.create("test-set", DistributedSet.class).thenAccept(set -> {
  set.add("value").thenRun(() -> {
    set.contains("value").thenAccept(result -> {
      if (result) {
        // Add a value with a TTL
        set.add("othervalue", Duration.ofSeconds(1));
      }
    });
  });
});
```

#### DistributedLock

**Synchronous API**

```java
DistributedLock lock = copycat.create("test-lock", DistributedLock.class).get();

// Lock the lock
lock.lock().join();

// Unlock the lock
lock.unlock().join();
```

**Asynchronous API**

```java
DistributedLock lock = copycat.create("test-lock", DistributedLock.class).thenAccept(lock -> {
  lock.lock().thenRun(() -> {
    // Do stuff...
    lock.unlock().thenRun(() -> {
      // Did stuff
    });
  });
});
```

#### DistributedLeaderElection

**Synchronous API**

```java
DistributedLeaderElection election = copycat.create("test-election", DistributedLeaderElection.class).get();

election.onElection(epoch -> {
  System.out.println("Elected leader!");
}).join();
```

**Asynchronous API**

```java
DistributedLeaderElection election = copycat.create("test-election", DistributedLeaderElection.class).thenAccept(election -> {
  election.onElection(epoch -> {
    System.out.println("Elected leader!");
  }).thenRun(() -> {
    System.out.println("Waiting for election");
  });
});
```

User Manual
===========

Documentation is still under active development. The following documentation is loosely modeled on the structure
of modules as illustrated in the [Javadoc][Javadoc]. Docs will be updated frequently until a release, so check
back for more! If you would like to request specific documentation, please
[submit a request](http://github.com/kuujo/copycat/issues)

1. [Introduction](#introduction)
   * [The CAP theorem](#the-cap-theorem)
   * [Copycat's consistency model](#consistency-model)
   * [Fault-tolerance in Copycat](#fault-tolerance)
   * [Project structure](#project-structure)
   * [Dependency management](#dependencies)
   * [The Copycat API](#the-copycat-api)
      * [Working with replicas](#copycatreplica)
      * [Working with clients](#copycatclient)
   * [Copycat's thread model](#thread-model)
      * [Asynchronous API usage](#asynchronous-api-usage)
      * [Synchronous API usage](#synchronous-api-usage)
   * [What are resources?](#resources)
      * [Persistence modes](#persistence-model)
      * [Consistency levels](#consistency-levels)
1. [Managing state in Copycat with resources](#distributed-resources)
   * [Working with collections](#distributed-collections)
      * [Sets](#distributedset)
      * [Maps](#distributedmap)
   * [Working with atomic variables](#distributed-atomic-variables)
      * [Atomic values](#distributedatomicvalue)
   * [Working with coordination tools](#distributed-coordination)
      * [Distributed locks](#distributedlock)
      * [Leader elections](#distributedleaderelection)
      * [Topic messaging](#distributedtopic)
   * [Writing custom resources](#custom-resources)
1. [I/O & Serialization](#io--serialization)
   * [Buffers](#buffers)
      * [Bytes](#bytes)
      * [Buffer pools](#buffer-pools)
   * [Serialization framework](#serialization)
      * [The serializer API](#serializer)
      * [Pooled object deserialization](#pooled-object-deserialization)
      * [Serializable type resolution](#serializable-type-resolution)
      * [Custom serializable types](#copycatserializable)
      * [Custom type serializers](#typeserializer)
   * [Storage framework](#storage)
      * [The commit log](#log)
      * [Log cleaning](#log-cleaning)
   * [Transport framework](#transports)
1. [Raft consensus algorithm](#raft-consensus-algorithm)
   * [Working with Raft clients](#raftclient)
      * [Client lifecycle](#client-lifecycle)
      * [Client sessions](#client-sessions)
   * [Working with Raft servers](#raftserver)
      * [Server lifecycle](#server-lifecycle)
   * [Raft commands (writes)](#commands)
   * [Raft queries (reads)](#queries)
      * [Query consistency](#query-consistency)
   * [Building a state machine](#state-machines)
      * [Registering state machine operations](#registering-operations)
      * [State machine commits](#commits)
      * [Accessing client sessions](#sessions)
      * [Cleaning the commit log](#commit-cleaning)
      * [Scheduling deterministic operations in the state machine](#deterministic-scheduling)
   * [Raft implementation details](#raft-implementation-details)
      * [Clients](#clients)
      * [Servers](#servers)
      * [State machines](#state-machines-1)
      * [Elections](#elections)
      * [Commands](#commands-1)
      * [Queries](#queries-1)
      * [Sessions](#sessions-1)
      * [Membership changes](#membership-changes)
      * [Log compaction](#log-compaction)
1. [Utilities](#utilities)
   * [Working with builders](#builders)
   * [Working with event listeners](#listeners)
   * [Working with execution contexts](#contexts)

## Introduction

Copycat is a framework for consistent distributed coordination. At the core of Copycat is a generic implementation
of the [Raft consensus algorithm][Raft]. On top of Raft, Copycat provides a high level API for creating and
managing arbitrary user-defined replicated state machines such as maps, sets, locks, or user-defined resources.
Resources can be created and modified by any replica or client in the cluster.

Copycat clusters consist of at least one [replica](#copycatreplica) and any number of [clients](#copycatclient).
*Replicas* are stateful nodes that actively participate in the Raft consensus protocol, and *clients* are stateless nodes
that modify system state remotely.

When a cluster is started, the replicas in the cluster coordinate with one another to elect a leader. Once a leader
has been elected, all state changes (i.e. writes) are proxied to the cluster leader. When the leader receives a write,
it persists the write to [disk](#storage) and replicates it to the rest of the cluster. Once a write has been received
and persisted on a majority of replicas, the write is committed and guaranteed not to be lost.

Because the Copycat cluster is dependent on a majority of the cluster being reachable to commit writes, the cluster can
tolerate a minority of the nodes failing. For this reason, it is recommended that each Copycat cluster have at least 3 or 5
replicas, and the number of replicas should always be odd in order to achieve the greatest level of fault-tolerance. The number
of replicas should be calculated as `2f + 1` where `f` is the number of failures to tolerate.

**So what's the difference between Copycat and those other projects?**

[ZooKeeper](https://zookeeper.apache.org/) - Copycat and ZooKeeper are both backed by a similar consensus-based
persistence/replication layer. But Copycat is a framework that can be embedded instead of depending on an external
cluster. Additionally, ZooKeeper's low-level primitives require complex recipes or other tools like
[Apache Curator](http://curator.apache.org/), whereas Copycat provides [high-level interfaces](#resources) for
common data structures and coordination tools like [locks](#distributedlock), [maps](#distributedmap), and
[leader elections](#distributedleaderelection), or the option to create [custom replicated state machines](#custom-resources).

[Hazelcast](http://hazelcast.org/) - Hazelcast is a fast, in-memory data grid that, like Copycat, exposes rich APIs
for operating on distributed objects. But whereas Hazelcast chooses
[availability over consistency in the face of a partition](https://en.wikipedia.org/wiki/CAP_theorem), Copycat is
designed to ensure that data is never lost in the event of a network partition or other failure. Like ZooKeeper,
this requires that Copycat synchronously replicate all writes to a majority of the cluster and persist writes to disk,
much like ZooKeeper.

### The CAP theorem

[The CAP theorem][CAP] is a frequently referenced theorem that states that it is impossible for a distributed
system to simultaneously provide *Consistency*, *Availability*, and *Partition tolerance*. All distributed systems
must necessarily sacrifice either consistency or availability, or some level of both, in the event of a partition.

By definition, high-throughput, high-availability distributed databases like [Hazelcast](http://hazelcast.org/) or
[Cassandra](http://cassandra.apache.org/) and other Dynamo-based systems fall under the *A* and *P* in the CAP theorem.
That is, these systems generally sacrifice consistency in favor of availability during network failures. In AP systems,
a network partition can result in temporary or even permanent loss of writes. These systems are generally designed to
store and query large amounts of data quickly.

Alternatively, systems like [ZooKeeper](https://zookeeper.apache.org/) which fall under the *C* and *P* in the CAP
theorem are generally designed to store small amounts of mission critical state. CP systems provide strong consistency
guarantees like [linearizability](https://en.wikipedia.org/wiki/Linearizability) and
[sequential consistency](https://en.wikipedia.org/wiki/Sequential_consistency) even in the face of failures, but that
level of consistency comes at a cost: availability. CP systems like ZooKeeper and Copycat are consensus-based and thus
can only tolerate the loss of a minority of servers.

### Consistency model

In terms of the CAP theorem, Copycat falls squarely in the CP range. That means Copycat provides configurable strong
consistency levels - [linearizability](https://en.wikipedia.org/wiki/Linearizability) for writes and reads, and
optional weaker [serializability](https://en.wikipedia.org/wiki/Serializability) for reads - for all operations.
Linearizability says that all operations must take place some time between their invocation and completion.
This means that once a write is committed to the cluster, all clients are guaranteed to see the resulting state. 

Consistency is guaranteed by [Copycat's implementation of the Raft consensus algorithm](#raft-consensus-algorithm).
Raft uses a [distributed leader election](https://en.wikipedia.org/wiki/Leader_election) algorithm to elect a leader.
The leader election algorithm guarantees that the server that is elected leader will have all the writes to the
cluster that have previously been successful. All writes to the cluster go through the cluster leader and are
*synchronously replicated to a majority of servers* before completion. Additionally, writes are sequenced in the
order in which they're submitted by the client (sequential consistency).

Unlike [ZooKeeper](https://zookeeper.apache.org/), Copycat natively supports linearizable reads as well. Much like
writes, linearizable reads must go through the cluster leader (which always has the most recent cluster state) and
may require contact with a majority of the cluster. For higher throughput, Copycat also allows reads from followers.
Reads from followers guarantee *serializable consistency*, meaning all clients will see state changes in the same order
but different clients may see different views of the state at any given time. Notably, *a client's view of the cluster
will never go back in time* even when switching between servers. Additionally, Copycat places a bound on followers
servicing reads: in order to service a read, a follower's log must be less than a heartbeat behind the leader's log.

*See the [Raft implementation details](#raft-implementation-details) for more information on consistency
in Copycat*

### Fault-tolerance

Because Copycat falls on the CP side of the CAP theorem, it favors consistency over availability, particularly under
failure. In order to ensure consistency, Copycat's [consensus protocol](#raft-consensus-algorithm) requires that
a majority of the cluster be alive and operating normally to service writes.

* A cluster of `1` replica can tolerate `0` failures
* A cluster of `2` replicas can tolerate `0` failures
* A cluster of `3` replicas can tolerate `1` failure
* A cluster of `4` replicas can tolerate `1` failure
* A cluster of `5` replicas can tolerate `2` failures

Failures in Copycat are handled by Raft's [leader election](https://en.wikipedia.org/wiki/Leader_election) algorithm.
When the Copycat cluster starts, a leader is elected. Leaders are elected by a round of voting wherein servers vote
for a candidate based on the [consistency of its log](#consistency-model).

In the event of a failure of the leader, the remaining servers in the cluster will begin a new election and a new leader
will be elected. This means for a brief period (seconds) the cluster will be unavailable.

In the event of a partition, if the leader is on the quorum side of the partition, it will continue to operate normally.
Alternatively, if the leader is on the non-quorum side of the partition, the leader will detect the partition (based on
the fact that it can no longer contact a majority of the cluster) and step down, and the servers on the majority side
of the partition will elect a new leader. Once the partition is resolved, nodes on the non-quorum side of the partition
will join the quorum side and receive updates to their log from the remaining leader.

### Project structure

Copycat is designed as a series of libraries that combine to form a framework for managing fault-tolerant state in
a distributed system. The project currently consists of 14 modules, each of which implements a portion
of the framework's functionality. The components of the project are composed hierarchically, so lower level
components can be used independently of most other modules.

A rough outline of Copycat's project hierarchy is as follows (from high-level to low-level):

* [Resources][Resource]
   * [Distributed collections][collections] (artifact ID: `copycat-collections`)
   * [Distributed atomic variables][atomic] (artifact ID: `copycat-atomic`)
   * [Distributed coordination tools][coordination] (artifact ID: `copycat-coordination`)
* [Copycat API][copycat] (artifact ID: `copycat`)
   * [Copycat Client][CopycatClient]
   * [Copycat Replica][CopycatReplica]
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
[Raft](#raft-consensus-algorithm), [I/O](#io--serialization), etc) can work independently of one another
and with minimal dependencies. To that end, *the core library has zero dependencies*. The only components
where dependencies are required is in custom `Transport` implementations, such as the [NettyTransport][NettyTransport].

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

Additionally, in order to facilitate communication between [clients](#copycatclient) and [replicas](#copycatreplica)
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
[replicas](#copycatreplica). This allows Copycat clients and servers to be embedded in applications that don't
care about the context. Resources can be created and operated on regardless of whether the local `Copycat`
instance is a [CopycatClient][CopycatClient] or [CopycatReplica][CopycatReplica].

#### CopycatReplica

The [CopycatReplica][CopycatReplica] is a [Copycat][Copycat] implementation that is responsible for receiving
creating and managing [resources](#resources) on behalf of other clients and replicas and receiving, persisting,
and replicating state changes for existing resources.

Users should think of replicas as stateful nodes. Because replicas are responsible for persisting and replicating
resource state changes, they require more configuration than [clients](#copycatclient).

To create a `CopycatReplica`, first you must create a [Transport](#transport) via which the replica will communicate
with other clients and replicas:

```java
Transport transport = new NettyTransport();
```

The [Transport][Transport] provides the mechanism through which replicas communicate with one another and
with clients. It is essential that all clients and replicas configure the same transport.

Once the transport is configured, the replica must be provided with a list of members to which to connect.
Cluster membership information is provided by configuring a `Members` list.

```java
Members members = Members.builder()
  .addMember(new Member(1, "123.456.789.1", 5555))
  .addMember(new Member(2, "123.456.789.2", 5555))
  .addMember(new Member(3, "123.456.789.3", 5555))
  .build();
```

Each member in the `Members` list must be assigned a unique `id` that remains consistent across
all clients and replicas in the cluster, and the local replica must be listed in the `Members` list.
In other words, if host `123.456.789.1` is member `1` on one replica, it must be listed as member
`1` on all replicas.

Finally, the `CopycatReplica` is responsible for persisting [resource](#resources) state changes.
To do so, the underlying [Raft server](#raftserver) writes state changes to a persistent [Log][Log].
Users must provide a [Storage][Storage] object which specifies how the underlying `Log` should
be created and managed.

To create a [Storage](#storage) object, use the storage [Builder](#builders) or for simpler configurations
simply pass the log directory into the `Storage` constructor:

```java
Storage storage = new Storage("logs");
```

Finally, with the [Transport][Transport], [Storage][Storage], and `Members` configured, create
the [CopycatReplica][CopycatReplica] with the replica [Builder](#builders) and `open()` the replica:

```java
Copycat copycat = CopycatReplica.builder(1, members)
  .withTransport(transport)
  .withStorage(storage)
  .build();

copycat.open().thenRun(() -> {
  System.out.println("Copycat started!");
});
```

Once created, the replica can be used as any `Copycat` instance to create and operate on [resources](#resources).

Internally, the `CopycatReplica` wraps a [RaftClient][RaftClient] and [RaftServer][RaftServer] to
communicate with other members of the cluster. For more information on the specific implementation
of `CopycatReplica` see the [RaftClient](#raftclient) and [RaftServer](#raftserver) documentation.

#### CopycatClient

The [CopycatClient][CopycatClient] is a [Copycat][Copycat] implementation that manages and operates
on [resources](#resources) by communicating with a remote cluster of [replicas](#copycatreplica).

Users should think of clients as stateless members of the Copycat cluster.

To create a `CopycatClient`, use the client [Builder](#builders) and provide a [Transport][Transport]
and a list of `Members` to which to connect:

```java
Members members = Members.builder()
 .addMember(new Member(1, "123.456.789.1", 5555))
 .addMember(new Member(2, "123.456.789.2", 5555))
 .addMember(new Member(3, "123.456.789.3", 5555))
 .build()

Copycat copycat = CopycatClient.builder(members)
  .withTransport(new NettyTransport())
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

### Resources

The true power of Copycat comes through provided and custom [Resource][Resource] implementation. Resources are
named distributed objects that are replicated and persisted in the Copycat cluster. Each name can be associated
with a single resource, and each resource is backed by a replicated state machine managed by Copycat's underlying
[implementation of the Raft consensus protocol](#raft-consensus-algorithm).

Resources are created by simply passing a `Resource` class to one of Copycat's `create` methods:

```java
DistributedMap<String, String> map = copycat.create("/test-map", DistributedMap.class);
```

Copycat uses the provided `Class` to create an associated [StateMachine](#state-machines) on each replica.
This allows users to create and integrate [custom resources](#custom-resources).

#### Persistence model

Copycat clients and replicas communicate with each other through [sessions](#sessions). Each session represents
a persistent connection between a single client and a complete Copycat cluster. Sessions allow Copycat to associate
resource state changes with clients, and this information can often be used to manage state changes in terms of
sessions as well.

Some Copycat resources expose a configurable `PersistenceMode` for resource state change operations. The
persistence mode specifies whether a state change is associated directly with the client's `Session`.
Copycat exposes two persistence modes:
* `PersistenceMode.PERSISTENT` - State changes persist across session changes
* `PersistenceMode.EPHEMERAL` - State changes are associated directly with the session that created them

The `EPHEMERAL` persistence mode allows resource state changes to be reflected only as long as the session
that created them remains alive. For instance, if a `DistributedMap` key is set with `PersistenceMode.EPHEMERAL`,
the key will disappear from the map when the session that created it expires or is otherwise closed.

#### Consistency levels

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
* `ConsistencyLevel.SERIALIZABLE` - Provides serializable consistency by allowing clients to read from followers and ensuring that
  clients see state progress monotonically

Overloaded methods with `ConsistencyLevel` parameters are provided throughout Copycat's resources wherever it makes sense.
In many cases, resources dictate the strongest consistency levels - e.g. [coordination](#distributed-coordination) - and
so weaker consistency levels are not allowed.

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

To create a `DistributedSet`, pass the class to `Copycat.create(String, Class)`:

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
set.add("Hello world!", PersistenceMode.EPHEMERAL).thenRun(() -> {
  // Close the Copycat instance to force the value to be removed from the set
  copycat.close();
});
```

#### DistributedMap

The [DistributedMap][DistributedMap] resources provides an asynchronous API similar to that of `java.util.Map`.

To create a `DistributedMap`, pass the class to `Copycat.create(String, Class)`:

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

To create a `DistributedAtomicValue`, pass the class to `Copycat.create(String, Class)`:

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

To create a `DistributedLock`, pass the class to `Copycat.create(String, Class)`:

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

To create a `DistributedLeaderElection`, pass the class to `Copycat.create(String, Class)`:

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

To create a `DistributedTopic`, pass the class to `Copycat.create(String, Class)`:

```java
copycat.<DistributedTopic<String>>create("/test-topic", DistributedTopic.class).thenAccept(topic -> {
  // Send and receive some messages with the topic
});
```

Once the topic has been created, users can send and receive messages. To send messages to the topic,
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
via `Copycat.create`, an associated state machine will be created on each Copycat replica, and operations submitted by the
resource instance will be applied to the replicated state machine. In that sense, think of a `Resource` instance
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

When a resource is created via `Copycat.create(String, Class)`, the `StateMachine` class returned
by the `Resource.stateMachine()` method will be constructed on each replica in the cluster. Once the state machine has been
created on a majority of the replicas, the resource will be constructed and the returned `CompletableFuture` completed.

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

All `Serializer` instance constructed by Copycat use `ServiceLoaderTypeResolver`. Copycat registers internal
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

Additionally, Copycat's Raft implementation uses [ServiceLoaderTypeResolver][ServiceLoaderTypeResolver] to register
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

To serialize and deserialize an object, simply write to and read from the passed in `BufferWriter` or `BufferReader`
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

When constructing a `RaftServer` or `CopycatReplica`, users must provide the server with a `Storage`
instance which controls the underlying `Log`. `Storage` objects are built via the storage [Builder](#builders):

```java
Storage storage = new Storage("logs");
```

```java
Storage storage = Storage.builder()
  .withDirectory("logs")
  .build();
```

#### Log

*Note: Much of the following is relevant only to Copycat internals*

Underlying the [Storage][Storage] API is the [Log][Log].

```java
Log log = storage.open();
```

The `Log` is an ordered and indexed list of entries stored on disk in a series of files called *segments*.
Each segment file represents range of entries in the overall log and is backed by a file-based [buffer](#buffers).
Entries are serialized to disk using Copycat's [serialization framework](#serialization).

Entries can only be appended to the log:

```java
try (MyEntry entry = log.create(MyEntry.class)) {
  entry.setFoo("foo");
  entry.setBar(1);
  log.append(entry);
}
```

Segment buffers are backed by an *offset index*. The offset index is responsible for tracking the offsets and positions
of entries in the segment. Indexes are built in memory from segment entries as they're written to disk. In order to
preserve disk/memory space, the index stores entry indices as offsets relative to the beginning of each segment. When
a segment is loaded from disk, the in-memory index is recreated from disk. Additionally, each segment is limited to a
maximum size of `Integer.MAX_VALUE` so that the position of an entry cannot exceed 4 bytes. This means each entry in the
index consumes only 8 bytes - 4 for the offset and 4 for the position.

When entries are read from a segment, the offset index is used to locate the starting position of the entry in the
segment file. To locate an entry, the index uses a [binary search algorithm](https://en.wikipedia.org/wiki/Binary_search_algorithm)
to locate the appropriate offset within the index buffer. Given the offset, the related *position* is used to seek
to the appropriate position in the segment file where the entry is read and deserialized.

Offset indexes are also responsible for tracking entries that have been [cleaned from the segment](#log-cleaning).
When entries are cleaned from the log, a flag is set in the owning segment's offset index to indicate that the
entry is awaiting compaction. Clean flags are stored in a bit set in memory, so each segment consumes
`[num entries] / 8` additional bytes of memory for delete bits.

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
the entries in the segment, rewriting live entries from the old segment to the new segment, and discarding
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

### RaftClient

The [RaftClient][RaftClient] provides an interface for submitting [commands](#commands) and [queries](#queries) to a cluster
of [Raft servers](#raftserver).

To create a client, you must supply the client [Builder](#builders) with a set of `Members` to which to connect.

```java
Members members = Members.builder()
  .addMember(new Member(1, "123.456.789.1" 5555))
  .addMember(new Member(2, "123.456.789.2" 5555))
  .addMember(new Member(3, "123.456.789.3" 5555))
  .build();
```

The provided `Members` do not have to be representative of the full Copycat cluster, but they do have to provide at
least one correct server to which the client can connect. In other words, the client must be able to communicate with
at least one `RaftServer` that is the leader or can communicate with the leader, and a majority of the cluster
must be able to communicate with one another in order for the client to register a new [Session](#session).

```java
RaftClient client = RaftClient.builder(members)
  .withTransport(new NettyTransport())
  .build();
```

Once a `RaftClient` has been created, connect to the cluster by calling `open()` on the client:

```java
client.open().thenRun(() -> System.out.println("Successfully connected to the cluster!"));
```

#### Client lifecycle

When the client is opened, it will connect to a random server and attempt to register its session. If session registration fails,
the client will continue to attempt registration via random servers until all servers have been tried. If the session cannot be
registered, the `CompletableFuture` returned by `open()` will fail.

#### Client sessions

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
RaftServer server = RaftServer.builder(1, members)
  .withTransport(new NettyTransport())
  .withStorage(new Storage("logs"))
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

#### Server lifecycle

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
the `close()` method must be called on a `RaftServer` instance. When `close()` is called, the member will submit a *leave* request
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
    return Consistency.SERIALIZABLE;
  }

}
```

The consistency level returned by the overridden `consistency()` method amounts to a *minimum consistency requirement*.
In many cases, a `SERIALIZABLE` query can actually result in `LINEARIZABLE` read depending the server to which a client submits
queries, but clients can only rely on the configured consistency level.

Copycat provides four consistency levels:
* `ConsistencyLevel.LINEARIZABLE` - Provides guaranteed linearizability by forcing all reads to go through the leader and
  verifying leadership with a majority of the Raft cluster prior to the completion of all operations
* `ConsistencyLevel.LINEARIZABLE_LEASE` - Provides best-effort optimized linearizability by forcing all reads to go through the
  leader but allowing most queries to be executed without contacting a majority of the cluster so long as less than the
  election timeout has passed since the last time the leader communicated with a majority
* `ConsistencyLevel.SERIALIZABLE` - Provides serializable consistency by allowing clients to read from followers and ensuring
  that clients see state progress monotonically

### State machines

State machines are the server-side representation of state based on a series of [commands](#commands) and [queries](#queries)
submitted to the Raft cluster.

**All state machines must be deterministic**

Given the same commands in the same order, state machines must always arrive at the same state with the same output.

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
recovery, state machine commands in the log will be replayed to the state machine. This is why it's so critical that state
machines be deterministic.

#### Registering operations

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

Sessions are representative of a single client's connection to the cluster. For each `Commit` applied to the state machine,
an associated `Session` is provided. State machines can use sessions to associate clients with state changes or even send
events back to the client through the session:

```java
protected Object put(Commit<PutCommand> commit) {
  commit.session().publish("putteded");
  return map.put(commit.operation().key(), commit.operation().value());
}
```

The `StateMachineContext` provides a view of the local server's state at the time a [command](#command) or [query](#queries)
is applied to the state machine. Users can use the context to access, for instance, the list of `Session`s currently registered
in the cluster.

To get the context, call the protected `context()` getter from inside the state machine:

```java
for (Session session : context().sessions()) {
  session.publish("Hello world!");
}
```

#### Commit cleaning

As commands are submitted to the cluster and applied to the Raft state machine, the underlying [log](#log) grows.
Without some mechanism to reduce the size of the log, the log would grow without bound and ultimately servers would
run out of disk space. Raft suggests a few different approaches of handling log compaction. Copycat uses the
[log cleaning](#log-cleaning) approach.

`Commit` objects are backed by entries in Copycat's replicated log. When a `Commit` is no longer needed by the
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

#### Deterministic scheduling

In addition to registering operation callbacks, the `StateMachineExecutor` also facilitates deterministic scheduling based on
the Raft replicated log.

```java
executor.schedule(() -> System.out.println("Every second"), Duration.ofSeconds(1), Duration.ofSeconds(1));
```

Because of the complexities of coordinating distributed systems, time does not advance at the same rate on all servers in
the cluster. What is essential, though, is that time-based callbacks be executed at the same point in the Raft log on all
nodes. In order to accomplish this, the leader writes an approximate `Instant` to the replicated log for each command.
When a command is applied to the state machine, the command's timestamp is used to invoke any outstanding scheduled callbacks.
This means the granularity of scheduled callbacks is limited by the minimum time between commands submitted to the cluster,
including session register and keep-alive requests. Thus, users should not rely on `StateMachineExecutor` scheduling for
accuracy.

### Raft implementation details

Copycat's implementation of the [Raft consensus algorithm][Raft] has been developed over a period of over two years. In most
cases, it closely follows the recommendations of the Diego Ongaro's [Raft dissertation](https://ramcloud.stanford.edu/~ongaro/thesis.pdf),
but sometimes it diverges from the norm. For instance, Raft dictates that all reads and writes be executed through the leader
node, but Copycat's Raft implementation supports per-request consistency levels that allow clients to sacrifice linearizability
and read from followers. Similarly, Raft literature recommends snapshots as the simplest approach to log compaction, but Copycat
prefers log cleaning to promote more consistent performance throughout the lifetime of a cluster.

It's important to note that when Copycat does diverge from the Raft norm, it does so using well-understood alternative
methods that are described in the Raft literature and frequently discussed on Raft discussion forums. Copycat does not
attempt to alter the fundamental correctness of the algorithm but rather extends it to promote usability in real-world
use cases.

The following documentation details Copycat's implementation of the Raft consensus algorithm and in particular the areas
in which the implementation diverges from the recommendations in Raft literature and the reasoning behind various decisions.

#### Clients

Copycat's Raft client is responsible for connecting to a Raft cluster and submitting [commands](#commands-1) and
[queries](#queries-1).

The pattern with which clients communicate with servers diverges slightly from that which is described in the Raft
literature. Copycat's Raft implementation uses client communication patterns that are closely modeled on those of
[ZooKeeper](https://zookeeper.apache.org/). Clients are designed to connect to and communicate with a single
server at a time. There is no correlation between the client and the Raft cluster's leader. In fact, clients never
even learn about the leader.

When a client is started, the client connects to a random server and attempts to [register a new session](#session-1).
If the registration fails, the client attempts to connect to another random server and register a new session again. In the
event that the client fails to register a session with any server, the client fails and must be restarted. Alternatively,
once the client successfully registers a session through a server, the client continues to submit [commands](#commands-1) and
[queries](#queries-1) through that server until a failure or shutdown event.

Once the client has successfully registered its session, it begins sending periodic *keep alive* requests to the cluster.
Clients are responsible for sending a keep alive request at an interval less than the cluster's *session timeout* to
ensure their session remains open.

If the server through which a client is communicating fails (the client detects a disconnection when sending a 
command, query, or keep alive request), the client will connect to another random server and immediately attempt to send
a new *keep alive* request. The client will continue attempting to commit a keep alive request until it locates another
live member of the Raft cluster.

#### Servers

Raft servers are responsible for participating in elections and replicating state machine [commands](#commands-1) and
[queries](#queries-1) through the Raft log.

Each Raft server maintains a single [Transport](#transport) *server* and *client* which is connected to each other
member of the Raft cluster at any given time. Each server uses a single-thread event loop internally to handle
requests. This reduces complexity and ensures that order is strictly enforced on handled requests.

#### State machines

Each server is configured with a [state machine](#state-machines-1) to which it applies committed [commands](#commands-1)
and [queries](#queries). State machines operations are executed in a separate *state machine* thread to ensure that
blocking state machine operations do not block the internal server event loop.

Servers maintain both an internal state machine and a user state machine. The internal state machine is responsible for
maintaining internal system state such as [sessions](#sessions-1) and [membership](#membership-changes) and applying
*commands* and *queries* to the user-provided `StateMachine`.

#### Elections

In addition to necessarily adhering to the typical Raft election process, Copycat's Raft implementation uses a pre-vote
protocol to improve availability after failures. The pre-vote protocol (described in section `4.2.3` of the
[Raft dissertation](https://ramcloud.stanford.edu/~ongaro/thesis.pdf)) ensures that only followers that are eligible to
become leaders can participate in elections. When followers elections timeout, prior to transitioning to candidates and
starting a new election, each follower polls the rest of the cluster to determine whether their log is up-to-date. Only
followers with up-to-date logs will transition to *candidate* and begin a new election, thus ensuring that servers that
can't win an election cannot disrupt the election process by resetting election timeouts for servers that can win an election.

#### Commands

Copycat's Raft implementation separates the concept of *writes* from *reads* in order to optimize the handling of each.
*Commands* are state machine operations which alter the state machine state. All commands submitted to a Raft cluster are
proxied to the leader, written to disk, and replicated through the Raft log.

When the leader receives a command, it writes the command to the log along with a client provided *sequence number*,
the *session ID* of the session which submitted the command, and an approximate *timestamp*. Notably, the *timestamp* is
used to provide a deterministic approximation of time on which state machines can base time-based command handling like
TTLs or other timeouts.

There are certain scenarios where sequential consistency can be broken by clients submitting commands via disparate
followers. If a client submits a command to server `A` which forwards it to server `B` (the leader), and then switches
servers and submits a command to server `C` which also forwards it to server `B`, it is conceivable that the command
submitted to server `C` could reach the leader prior to the command submitted via server `A`. If those commands are
committed to the Raft log in the order in which they're received by the leader, that will violate sequential consistency
since state changes will no longer reflect the client's program order.

Because of the pattern with which clients communicate with servers, this may be an unlikely occurrence. Clients only
switch servers in the event of a server failure. Nevertheless, failures are when it is most critical that a systems
maintain their guarantees, so leaders are responsible for ensuring that commands received by a client are logged in
sequential order as specified by that client.

When a client submits a command to the cluster, it tags the command with a monotonically increasing *sequence* number.
The sequence number is used for two purposes. First, it is used to sequence commands as they're written to the Raft
log. If a leader receives a command with a sequence number greater than `1 + previousSequence` then the command request
will be queued for handling once the commands are properly sequenced. This ensures that commands are written to the
Raft log in the order in which they were sent by the client regardless of the route through which they travelled to the
leader.

Note, however, that commands are not rejected if the *sequence* number is less than that of commands previously submitted
by the client. The reasoning behind this is to handle a specific failure scenario. [Sessions](#sessions-1) provide linearizability
for commands submitted to the cluster by clients by using the same *sequence* number to store command output and deduplicate
commands as they're applied to the state machine. If a client submits a command to a server that fails, the client doesn't
necessarily know whether or not the command succeeded. Indeed, the command could have been replicated to a majority of the
cluster prior to the server failure. In that case, the command would ultimately be committed and applied to the state machine,
but the client would never receive the command output. Session-based linearizability ensures that clients can still read output
for commands resubmitted to the cluster, but that requires that leaders allow commands with old *sequence* numbers to be logged
and replicated.

Finally, [queries](#queries-1) are optionally allowed to read stale state from followers. In order to do so in a manner that
ensures serializability (state progresses monotonically) when the client switches between servers, the client needs to have a
view of the most recent state for which it has received output. When commands are committed and applied to the user-provided
state machine, command output is [cached in memory for linearizability](#sessions-1) and the command output returned to the
client along with the *index* of the command. Thereafter, when the client submits a query to a follower, it will ensure that
it does not see state go back in time by indicating to the follower the highest index for which it has seen state.

*For more on linearizable semantics, see the [sessions](#sessions-1) documentation*

#### Queries

*Queries* are state machine operations which read state machine state but do not alter it. This is critical because queries
are never logged to the Raft log or replicated. Instead, queries are applied either on a follower or the leader based on the
configured per-query *consistency level*.

When a query is submitted to the Raft cluster, as with all other requests the query request is sent to the server to which
the client is connected. The server that receives the query request will handle the query based on the query's configured
*consistency level*. If the server that receives the query request is not the leader, it will evaluate the request to
determine whether it needs to be proxied to the leader:

If the query is `LINEARIZABLE` or `LINEARIZABLE_LEASE`, the server will forward the query to the leader.

`LINEARIZABLE` queries are handled by the leader by contacting a majority of the cluster before servicing the query.
When the leader receives a linearizable read, if the leader is in the process of sending *AppendEntries* RPCs to followers
then the query is queued for the next heartbeat. On the next heartbeat iteration, once the leader has successfully contacted
a majority of the cluster, queued queries are applied to the user-provided state machine and the leader responds to their
respective requests with the state machine output. Batching queries during heartbeats reduces the overhead of synchronously
verifying the leadership during reads.

`LINEARIZABLE_LEASE` queries are handled by the leader under the assumption that once the leader has verified its leadership
with a majority of the cluster, it can assume that it will remain the leader for at least an election timeout. When a
lease-based linearizable query is received by the leader, it will check to determine the last time it verified its leadership
with a majority of the cluster. If more than an election timeout has elapsed since it contacted a majority of the cluster,
the leader will immediately attempt to verify its leadership before applying the query to the user-provided state machine.
Otherwise, if the leadership was verified within an election timeout, the leader will immediately apply the query to the
user-provided state machine and respond with the state machine output.

If the query is `SERIALIZABLE`, the receiving server performs a consistency check to ensure that its log is not too far
out-of-sync with the leader to reliably service a query. If the receiving server's log is in-sync, it will wait until
the log is caught up until the last index seen by the requesting client before servicing the query. When queries are
submitted to the cluster, the client provides a *version* number which specifies the highest index for which
it has seen a response. Awaiting that index when servicing queries on followers ensures that state does not go back
in time if a client switches servers. Once the server's state machine has caught up to the client's *version*
number, the server applies the query to its state machine and response with the state machine output.

Clients' *version* numbers are based on feedback received from the cluster when submitting commands and queries.
Clients receive *version* numbers for each command and query submitted to the cluster. When a client submits a command
to the cluster, the command's *index* in the Raft replicated log will be returned to the client along with the output. This
is the client's *version* number. Similarly, when a client submits a query to the cluster, the server that services the query
will respond with the query output and the server's *lastApplied* index as the *version* number.

Log consistency for inconsistent queries is determined  by checking whether the server's log's `lastIndex` is greater than
or equal to the `commitIndex`. That is, if the last *AppendEntries* RPC received by the server did not contain a `commitIndex`
less than or equal to the log's `lastIndex` after applying entries, the server is considered out-of-sync and queries are
forwarded to the leader.

#### Sessions

Copycat's Raft implementation uses sessions to provide linearizability for commands submitted to the cluster. Sessions
represent a connection between a `RaftClient` and a `RaftServer` and are responsible for tracking communication between
them.

Certain failure scenarios can conceivably result on client commands being applied to the state machine more than once. For
instance, if a client submits a command to the cluster and the leader logs and replicates the command before failing, the
command may actually be committed and applied to state machines on each node. In that case, if the client resubmits the
command to another node, the command will be applied twice to the state machine. Sessions solve this problem by temporarily
storing command output in memory and deduplicating commands as they're applied to the state machine.

##### How it works

When a client connects to Copycat's Raft cluster, the client chooses a random Raft server to which to connect and
submits a *register* request to the cluster. The *register* request is forwarded to the Raft cluster leader if one exists,
and the leader logs and replicates the registration through the Raft log. Entries are logged and replicated with an
approximate *timestamp* generated by the leader.

Once the *register* request has been committed, the leader replies to the request with the *index* of the registration
entry in the Raft log. Thereafter, the registration *index* becomes the globally unique *session ID*, and the client must
submit commands and queries using that index.

Once a session has been registered, the client must periodically submit *keep alive* requests to the Raft cluster. As with
*register* requests, *keep alive* requests are logged and replicated by the leader and ultimately applied to an internal
state machine. Keep alive requests also contain an additional `sequence` number which specifies the last command for which
the client received a successful response, but more on that in a moment.

Once a session has been registered, the client must submit all commands to the cluster with an active *session ID* and
a monotonically increasing *sequence number* for the session. The *session ID* is used to associate the command with a
set of commands stored in memory on the server, and the *sequence number* is used to deduplicate commands committed to the
Raft cluster. When commands are applied to the user-provided [state machine](#state-machines), the command output is stored
in an in-memory map of results. If a command is committed with a *sequence number* that has already been applied to the
state machine, the previous output will be returned to the client and the command will not be applied to the state machine
again.

On the client side, in addition to tagging requests with a monotonically increasing *sequence number*, clients store the
highest sequence number for which they've received a successful response. When a *keep alive* request is sent to the cluster,
the client sends the last sequence number for which they've received a successful response, thus allowing servers to remove
command output up to that number.

##### Server events

In addition to providing linearizable semantics for commands submitted to the cluster by clients, sessions are also used
to allow servers to send events back to clients. To do so, Copycat exposes a `Session` object to the Raft state machine for
each [command](#commands) or [query](#queries) applied to the state machine:

```java
protected Object get(Commit<GetQuery> commit) {
  commit.session().publish("got it!");
  return map.get(commit.operation().key());
}
```

Rather than connecting to the leader, Copycat's Raft clients connect to a random node and writes are proxied to the leader.
When an event is published to a client by a state machine, only the server to which the client is connected will send the
event ot the client, thus ensuring the client only receives one event from the cluster. In the event that the client is
disconnected from the cluster (e.g. switching servers), events published through sessions are linearized in a manner similar
to that of commands.

When an event is published to a client by a state machine, the event is queue in memory with a sequential ID for the session.
Clients keep track of the highest sequence number for which they've received an event and send that sequence number back to
the cluster via *keep alive* requests. As keep alive requests are logged and replicated, servers clear acknowledged events
from memory. This ensures that all servers hold unacknowledged events in memory until they've been received by the client
associated with a given session. In the event that a session times out, all events are removed from memory.

*For more information on sessions in Raft, see section 6.3 of Diego Ongaro's
[Raft dissertation](https://ramcloud.stanford.edu/~ongaro/thesis.pdf)*

#### Membership changes

Membership changes are the process through which members are added to and removed from the Raft cluster. This is a notably
fragile process due to the nature of consensus. Raft requires that an entry be stored on a majority of servers in order to
be considered committed. But if new members are added to the cluster too quickly, this guarantee can be broken. If 3 members
are added to a 2-node cluster and immediately begin participating in the voting protocol, the three new members could potentially
form a consensus and any commands that were previously committed to the cluster (requiring `1` node for commitment) would no
longer be guaranteed to persist following an election.

Throughout the evolution of the Raft consensus algorithm, two major approaches to membership changes have been suggested
for handling membership changes. The approach originally suggested in Raft literature used joint-consensus to ensure that
two majorities could not overlap each other during a membership change. But more recent literature suggests adding or removing
a single member at a time in order to avoid the join-consensus problem altogether. Copycat takes the latter approach.

Copycat's configuration change algorithm is designed to allow servers to arbitrarily join and leave the cluster. When a cluster
is started for the first time, the configuration provided to each server at startup is used as the base cluster configuration.
Thereafter, servers that are joining the cluster are responsible for coordinating with the leader to join the cluster.

Copycat performs membership changes by adding two additional states to Raft servers: the *join* and *leave* states.
When a Raft server is started, the server first transitions into the *join* state. While in the join state, the server
attempts to determine whether a cluster is already available by attempting to contact a leader of the cluster. If the
server fails to contact a leader, it transitions into the *follower* state and continues with normal operation of the
Raft consensus protocol.

If a server in the *join* state does successfully contact a leader, it submits a *join* request to the leader, requesting
to join the cluster. The leader may or may not already know about the joining member. When the leader receives a *join*
request from a joining member, the leader immediately logs a *join* entry, updates its configuration, and replicates the
entry to the rest of the cluster.

The leader is responsible for maintaining two sets of members: *passive* members and *active* members. *Passive* members
are members that are in the process of joining the cluster but cannot yet participate in elections, but in all other functions,
including replication via *AppendEntries* RPCs, they function as normal Raft servers. The period in which a member is in
the *passive* state is intended to catch up the joining member's log enough that it can safely transition to a full member
of the cluster and participate in elections. The leader is responsible for determining when a *passive* member has caught
up to it based on the passive member's log. Once the passive member's log contains the last *commitIndex* sent to that member,
it is considered to be caught up. Once a member is caught up to the leader, the leader will log a second entry to the log
and replicate the configuration, resulting in the joining member being promoted to *active* state. Once the *passive* member
receives the configuration change, it transitions into the *follower* state and continues normal operation.

#### Log compaction

From the [Raft dissertation](https://ramcloud.stanford.edu/~ongaro/thesis.pdf):

> Raft’s log grows during normal operation as it incorporates more client requests. As it grows larger,
> it occupies more space and takes more time to replay. Without some way to compact the log, this
> will eventually cause availability problems: servers will either run out of space, or they will take too
> long to start. Thus, some form of log compaction is necessary for any practical system.

Raft literature suggests several ways to address the problem of logs growing unbounded. The most common of the log
compaction methodologies is snapshots. Snapshotting compacts the Raft log by storing a snapshot of the state machine
state and removing all commands applied to create that state. As simple as this sounds, though, there are some complexities.
Servers have to ensure that snapshots are reflective of a specific point in the log even while continuing to service
commands from clients. This may require that the process be forked for snapshotting or leaders step down prior to
taking a snapshot. Additionally, if a follower falls too far behind the leader (the follower's log's last index is
less than the leader's snapshot index), additional mechanisms are required to replicate snapshots from the leader to the
follower.

Alternative methods suggested in the Raft literature are mostly variations of log cleaning. Log cleaning is the process
of removing individual entries from the log once they no longer contribute to the state machine state. The disadvantage
of log cleaning - in particular for an abstract framework like Copycat - is that it adds additional complexity in requiring
state machines to keep track of commands that no longer apply to the state machine's state. This complexity is multiplied
by the delicacy handling tombstones. Commands that result in the absence of state must be carefully managed to ensure they're
applied on *all* Raft servers. Nevertheless, log cleaning provides significant performance advantages by writing logs
efficiently in long sequential strides.

Copycat opted to sacrifice some complexity to state machines in favor of more efficient log compaction. Copycat's Raft
log is written to a series of segment files and individually represent a subset of the entries in the log. As entries
are written to the log and associated commands are applied to the state machine, state machines are responsible for
explicitly cleaning the commits from the log. The log compaction algorithm is optimized to select segments based on the
number of commits marked for cleaning. Periodically, a series of background threads will rewrite segments of the
log in a thread-safe manner that ensures all segments can continue to be read and written. Whenever possible, neighboring
segments are combined into a single segment.

![Raft log compaction](http://s21.postimg.org/fvlvlg9lz/Raft_Compaction_New_Page_3.png)

*For more information on how commits are cleaned see the [log documentation](#log).*

This compaction model means that Copycat's Raft protocol must be capable of accounting for entries missing from the log.
When entries are replicated to a follower, each entry is replicated with its index so that the follower can write entries
to its own log in the proper sequence. Entries that are not present in a server's log or in an *AppendEntries* RPC are simply
skipped in the log. In order to maintain consistency, it is critical that state machines implement log cleaning correctly.

The most complex case for state machines to handle is tombstone commands. It's fairly simple to determine when a stateful
command has been superseded by a more recent command. For instance, consider this history:

```
put 1
put 3
```

In the scenario above, once the second `put` command has been applied to the state machine, it's safe to remove the first
`put` from the log. However, for commands that result in the absence of state (tombstones), cleaning the log is not as simple:

```
put 1
put 3
delete 3
```

In the scenario above, the first two `put` commands must be cleaned from the log before the final `delete` command can be
cleaned. If the `delete` is cleaned from the log prior to either `put` and the server fails and restarts, the state machine
will result in a non-deterministic state. Thus, state machines must ensure that commands that created state are cleaned
before a command that results in the absence of that state.

Furthermore, it is essential that the `delete` command be replicated on *all* servers in the cluster prior to being cleaned
from any log. If, for instance, a server is partitioned when the `delete` is committed, and the `delete` is cleaned from the
log prior to the partition healing, that server will never receive the tombstone and thus not clean all prior `put` commands.

Some systems like [Kafka](http://kafka.apache.org/) handle tombstones by aging them out of the log after a large interval
of time, meaning tombstones must be handled within a bounded timeframe. Copycat opts to ensure that tombstones have been
persisted on all servers prior to cleaning them from the log.

In order to handle log cleaning for tombstones, Copycat extends the Raft protocol to keep track of the highest index in the
log that has been replicated on *all* servers in the cluster. During normal *AppendEntries* RPCs, the leader sends a
*global index* which indicates the highest index represented on all servers in the cluster based on the leader's
`matchIndex` for each server. This global index represents the highest index for which tombstones can be safely removed from
the log.

Given the global index, state machines must use the index to determine when it's safe to remove a tombstone from the log.
But Copycat doesn't actually even expose the global index to the state machine. Instead, Copycat's state machines are designed
to clean tombstones only when there are no prior commits that contribute to the state being deleted by the tombstone. It does
so by periodically replaying globally committed commands to the state machine, allowing it to remove commits that have no
prior state.

Consider the tombstone history again:

```
put 1
put 3
delete 3
```

The first time that the final `delete` command is applied to the state machine, it will have marked the first two
`put` commands for deletion from the log. At some point in the future after segment to which the associated entries
belong are cleaned, the history in the log will contain only a single command:

```
delete 3
```

At that point, if commands are replayed to the state machine, the state machine will see that the `delete` does not
actually result in the absence of state since the state never existed to begin with. Each server in the cluster will
periodically replay early entries that have been persisted on all servers to a clone of the state machine to allow
it to clean tombstones that relate to invalid state. This is a clever way to clean tombstones from the log by essentially
*never* cleaning tombstones that delete state, and instead only cleaning tombstones that are essentially irrelevant.

*See chapter 5 of Diego Ongaro's [Raft dissertation](https://ramcloud.stanford.edu/~ongaro/thesis.pdf) for more on
log compaction*

## Utilities

The following documentation explains the usage of various utility APIs provided by the `copycat-common` module.

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

### [User Manual](#user-manual-1)

## [Javadoc][Javadoc]

[Javadoc]: http://kuujo.github.io/copycat/api/0.6.0/
[CAP]: https://en.wikipedia.org/wiki/CAP_theorem
[Raft]: https://raft.github.io/
[Executor]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/Executor.html
[CompletableFuture]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html
[collections]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/collections.html
[atomic]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/atomic.html
[coordination]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/coordination.html
[copycat]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat.html
[protocol]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/raft/protocol.html
[io]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io.html
[serializer]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/serializer.html
[transport]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/transport.html
[storage]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/io/storage.html
[utilities]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/util.html
[Copycat]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/Copycat.html
[CopycatReplica]: http://kuujo.github.io/copycat/api/0.6.0/net/kuujo/copycat/CopycatReplica.html
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
