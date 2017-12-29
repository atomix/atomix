# [Atomix][Website]

[![Build Status](https://travis-ci.org/atomix/atomix.svg)](https://travis-ci.org/atomix/atomix)
[![Coverage Status](https://coveralls.io/repos/github/atomix/atomix/badge.svg?branch=master)](https://coveralls.io/github/atomix/atomix?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.atomix/atomix/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.atomix/atomix)
[![Gitter](https://img.shields.io/badge/GITTER-join%20chat-green.svg)](https://gitter.im/atomix/atomix)

**A framework for building fault-tolerant distributed systems for the JVM**

*Atomix 2.1 documentation is currently under development. Check the website for updates!*

1. [Overview](#overview)
1. [Background](#background)
1. [Architecture](#architecture)
   * [The Atomix cluster](#the-atomix-cluster)
1. [Java API](#java-api)
   * [Cluster setup](#bootstrapping-the-cluster)
      * [Bootstrapping the cluster](#bootstrapping-the-cluster)
      * [Connecting a client node](#connecting-a-client-node)
   * [Cluster management](#cluster-management)
      * [Failure detection](#failure-detection)
   * [Cluster communication](#cluster-communication)
      * [Direct messaging](#direct-messaging)
         * [Registering message subscribers](#registering-message-subscribers)
         * [Sending messages](#sending-messages)
         * [Message serialization](#message-serialization)
      * [Publish-subscribe messaging](#publish-subscribe-messaging)
   * [Coordination primitives](#coordination-primitives)
      * [`DistributedLock`](#distributedlock)
      * `LeaderElection`
      * [`LeaderElector`](#leaderelector)
      * `WorkQueue`
   * [Data primitives](#data-primitives)
      * `AtomicCounter`
      * `AtomicCounterMap`
      * `AtomicValue`
      * [`ConsistentMap`](#consistentmap)
      * `ConsistentTreeMap`
      * `ConsistentMultimap`
      * `DistributedSet`
      * `DocumentTree`
   * [Transactions](#transactions)
      * [`TransactionalMap`](#transactionalmap)
      * [`TransactionalSet`](#transactionalset)
   * [Serialization](#serialization)
1. [REST API](#rest-api)
   * [Running a standalone Atomix agent](#running-a-standalone-atomix-agent)
   * [Bootstrapping a standalone cluster](#bootstrapping-a-standalone-cluster)
   * [Starting a client node](#starting-a-client-node)
   * [REST API examples](#rest-api-examples)
1. [Docker container](#docker)
1. [Interactive CLI](#interactive-cli)
   * [Starting the CLI](#starting-the-cli)
   * [Print help](#print-help)
   * [Running a command](#running-a-command)

## Overview

Atomix 2.1 is a fully featured framework for building fault-tolerant distributed systems.
Combining ZooKeeper's consistency with Hazelcast's usability and performance, Atomix uses a set of
custom communication APIs, a sharded Raft cluster, and a multi-primary protocol to provide
a series of high-level primitives for building distributed systems. These primitives include:
* [Cluster management](#cluster-management) and failure detection
* [Cluster communication](#cluster-communication) (direct and pub-sub) via Netty
* Strongly consistent reactive [distributed coordination primitives](#coordination-primitives) (locks, leader elections, etc)
* Efficient partitioned distributed [data structures](#data-primitives) (maps, sets, trees, etc)
* [REST API](#rest-api)
* [Interactive CLI](#interactive-cli)

## Background

Atomix was originally conceived in 2014 along with its sister project [Copycat](http://github.com/atomix/copycat)
(deprecated) as a hobby project. Over time, Copycat grew into a mature implementation of the Raft consensus
protocol, and both Copycat and Atomix were put into use in various projects. In 2017, development of a new version
was begun, and Copycat and Atomix were combined in Atomix 2.x. Additionally, significant extensions to the projects
originally developed for use in [ONOS](http://onosproject.org) were migrated into Atomix 2.x. Atomix is now
maintained as a core component of ONOS by the [Open Networking Foundation](http://opennetworking.org).

## Architecture

At its core, Atomix is built on a series of replicated state machines backed by consensus and primary-backup
protocols. The most critical component of Atomix is one of the most complete implementations of the
[Raft consensus protocol](https://raft.github.io/). A single Raft cluster is used for internal coordination,
and multiple additional Raft clusters are used to store state for consistent primitives. The primary-backup
protocol uses the core Raft cluster to elect and balance primaries and backups for all data partitions.
Primitives can be configured for consistency, persistence, and replication, modifying the underlying
protocol according to desired semantics.

### The Atomix cluster

Atomix clusters consist of two types of nodes:
* `DATA` nodes store persistent and ephemeral primitive state
* `CLIENT` nodes do not store any state but must connect to `DATA` nodes to store state remotely

Primitive partitions (both Raft and primary-backup) are evenly distributed among the `DATA` nodes
in a cluster. Initially, an Atomix cluster is formed by bootstrapping a set of `DATA` nodes. Thereafter,
additional `DATA` or `CLIENT` nodes may join and leave the cluster at will by simply starting and
stopping `Atomix` instances. Atomix provides a `ClusterService` that can be used to learn about new
`CLIENT` and `DATA` nodes joining and leaving the cluster.

## Java API

### Bootstrapping the cluster

Atomix relies heavily upon builder APIs to build high-level objects used to communicate and coordinate
distributed systems.

To create a new Atomix instance, create an Atomix builder:

```java
Atomix.Builder builder = Atomix.builder();
```

The builder should be configured with the local node configuration:

```java
builder.withLocalNode(Node.builder("server1")
  .withType(Node.Type.DATA)
  .withEndpoint(Endpoint.from("localhost", 5000))
  .build());
```

In addition to configuring the local node information, each instance must be configured with a
set of _bootstrap nodes_ from which to form a cluster. When first starting a cluster, all instances
should provide the same set of bootstrap nodes. Bootstrap nodes _must_ be `DATA` nodes:

```java
builder.withBootstrapNodes(
  Node.builder("server1")
    .withType(Node.Type.DATA)
    .withEndpoint(Endpoint.from("localhost", 5000))
    .build(),
  Node.builder("server2")
    .withType(Node.Type.DATA)
    .withEndpoint(Endpoint.from("localhost", 5001))
    .build(),
  Node.builder("server3")
    .withType(Node.Type.DATA)
    .withEndpoint(Endpoint.from("localhost", 5002))
    .build());
```

Additionally, the Atomix instance can be configured with the data directory, the number of Raft 
partitions, the number of primary-backup partitions, and other options. Once the instance has
been configured, build the instance by calling `build()`:

```java
Atomix atomix = builder.build();
```

Finally, call `start()` on the instance to start the node:

```java
atomix.start().join();
```

**Note that in order to form a cluster, a majority of instances must be started concurrently
to allow Raft partitions to form a quorum.** The future returned by the `start()` method will
not be completed until all partitions are able to form. If your `Atomix` instance is blocking
indefinitely at startup, ensure you enable `DEBUG` logging to debug the issue.

### Connecting a client node

Atomix also supports _client nodes_ which can connect to an Atomix cluster and operate on distributed
primitives but does not itself store primitive state. To create a client node, simply indicate in the
node builder that the node is a `CLIENT`:

```java
Atomix atomix = Atomix.builder()
  .withLocalNode(Node.builder("client")
    .withType(Node.Type.CLIENT)
    .withEndpoint(Endpoint.from("localhost", 5003))
    .build())
  .withBootstrapNodes(
      Node.builder("server1")
        .withType(Node.Type.DATA)
        .withEndpoint(Endpoint.from("localhost", 5000))
        .build(),
      Node.builder("server2")
        .withType(Node.Type.DATA)
        .withEndpoint(Endpoint.from("localhost", 5001))
        .build(),
      Node.builder("server3")
        .withType(Node.Type.DATA)
        .withEndpoint(Endpoint.from("localhost", 5002))
        .build())
  .build();

atomix.start().join();
```

This example connects a client node aptly named `client` to a set of data nodes. Once the instance
is started, the client node will be visible to all data nodes and vice versa, and primitives created
by the client node will be managed by the data nodes.

## Cluster management

Atomix provides a set of APIs for discovering the nodes in the cluster. The `ClusterMetadataService`
provides information about the set of data nodes in the cluster. The `ClusterService` provides information
about all nodes in the cluster, including the local node, data nodes, and client nodes.

To get the set of nodes in the cluster, use the `ClusterService`:

```java
Set<Node> nodes = atomix.clusterService().getNodes();
```

### Failure detection

The `Node` objects provided by the `ClusterService` provide a `Node.State` that can indicates the
liveliness of the given node. The cluster service uses a phi accrual failure detection algorithm
internally to detect failures, and nodes' states are updated as failures are detected:

```java
Node.State state = atomix.clusterService().getNode(NodeId.from("foo")).state();
```

Additionally, listeners can be added to the `ClusterService` to react to changes to both the set
of nodes in the cluster and the states of individual nodes:

```java
atomix.clusterService().addListener(event -> {
  if (event.type() == ClusterEvent.Type.NODE_ACTIVATED) {
    // A node's state was changed to ACTIVE
  } else if (event.type() == ClusterEvent.Type.NODE_DEACTIVATED) {
    // A node's state was changed to INACTIVE
  }
});
```

## Cluster communication

Atomix provides three services that can be used for general cluster communication:
* `MessagingService` is a low-level IP address based communication API
* `ClusterCommunicationService` is a high-level point-to-point/unicast/multicast/broadcast messaging API
* `ClusterEventService` is a high-level messaging API modelled on the Vert.x event bus that abstracts
producers from consumers

The default implementation of communication abstractions uses [Netty](https://netty.io/) for all
inter-node communication.

### Direct messaging

Atomix provides the `ClusterCommunicationService` for point-to-point messaging between Atomix
nodes. It provides support for unicast, multicast, broadcast, and request-reply messaging patterns.

#### Registering message subscribers

To register a message subscriber, use the `subscribe` methods:

```java
atomix.messagingService().subscribe("test", message -> {
  return CompletableFuture.completedFuture(message);
});
```

Three types of subscribers can be registered:
* A synchronous subscriber that returns a result and must provide an `Executor` on which to consume messages
* An asynchronous subscriber that must return `CompletableFuture`
* A consumer that must provide an `Executor` on which to consume messages

#### Sending messages

As noted above, messages can be sent using a variety of different communication patterns:
* `unicast` sends a message to a single node without awaiting a response
* `multicast` sends a message to a set of nodes without awaiting any responses
* `broadcast` sends a message to all nodes known to the local `ClusterService` without awaiting any responses
* `sendAndReceive` sends a message to a single node and awaits a response

```java
// Send a request-reply message to node "foo"
atomix.messagingService().send("test", "Hello world!", NodeId.from("foo")).thenAccept(response -> {
  System.out.println("Received " + response);
});
```

#### Message serialization

The `ClusterCommunicationService` uses a default serializer to serialize a variety of core data
structures, but often custom objects need to be communicated across the wire. The `ClusterCommunicationService`
provides overloaded methods for providing arbitrary message encoders/decoders for requests/replies:

```java
Serializer serializer = Serializer.using(KryoNamespace.builder()
  .register(KryoNamespaces.BASIC)
  .register(NodeId.class)
  .register(ClusterHeartbeat.class)
  .build());

ClusterHeartbeat heartbeat = new ClusterHeartbeat(atomix.clusterService().getLocalNode().id());
atomix.messagingService().broadcast("test", heartbeat, serializer::encode);
```

### Publish-subscribe messaging

Publish-subscribe messaging is done using the `ClusterEventService` API, which is closely modelled
on the `ClusterCommunicationService` API. Indeed, while the two appear to be almost the exact same,
their semantics differ significantly. Rather than sending messages to specific nodes using `NodeId`s,
the `ClusterEventService` actually replicates subscriber information and routes messages internally.
Point-to-point messages sent via the `ClusterEventService` are delivered in a round-robin fashion,
and multicast messages do not require any specific node information. This separates specific nodes from
senders.

```java
// Add an event service subscriber
atomix.eventingService().subscribe("test", message -> {
  return CompletableFuture.completedFuture(message);
});

// Send a request-reply message via the event service
atomix.eventingService().send("test", "Hello world!").thenAccept(response -> {
  System.out.println("Received " + response);
});

// Broadcast a message to all event subscribers
atomix.eventingService().broadcast("test", "Hello world!");
```

## Coordination primitives

Coordination primitives can be used to elect leaders and synchronize access to shared resources
in the cluster. These primitives typically require the use of consensus but allow certain consistency
models to be relaxed, e.g. on reads. Coordination primitives include:
* [`DistributedLock`](#distributedlock) - coarse grained fair distributed lock
* `LeaderElection` - single leader election
* [`LeaderElector`](#leaderelector) - multiple leader election with automatic leader balancing
* `WorkQueue` - distributed persistent work queue with ack/fail mechanisms

### DistributedLock

As with many other objects in Atomix, primitives are constructed using a builder pattern. All
core primitive builders are exposed via the `Atomix` API:

```java
DistributedLock lock = atomix.lockBuilder("test-lock")
  .withLockTimeout(Duration.ofSeconds(2))
  .build();
```

The _lock timeout_ is the approximate time it takes to determine whether a lock has failed.
If a lock's underlying Raft session is expired, the lock will be released and granted to the
next waiting process.

```java
lock.lock();
try {
  // Do something
} finally {
  lock.unlock();
}
```

As with all Atomix primitives, a fully asynchronous version of the lock can be constructed by
simply calling the `async()` method on the lock:

```java
AsyncDistributedLock asyncLock = lock.async();
lock.lock().thenRun(() -> {
  // Do something
  lock.unlock().thenRun(() -> {
    // Unlocked
  });
});
```

### LeaderElector

The `LeaderElector` primitive is used to elect multiple leaders within a cluster, automatically
balancing leaders among all instances of the primitive across the cluster.

```java
LeaderElector<NodeId> elector = atomix.leaderElectorBuilder("test-elector")
  .withElectionTimeout(Duration.ofSeconds(2))
  .build();
```

As with `DistributedLock`, the `LeaderElector` (and `LeaderElection`) primitive supports a timeout
after which inactive leaders will be demoted and a new leader will be elected, e.g. in the case of
a crash or network partition. Similarly, as with all primitives, an `AsyncLeaderElector` can be
created via the `async()` getter:

```java
AsyncLeaderElector<NodeId> asyncElector = elector.async();
```

To enter into an election, use the `run` method:

```java
asyncElector.run("foo", atomix.clusterService().getLocalNode().id()).thenAccept(leadership -> {
  if (leadership.leader().id().equals(atomix.clusterService().getLocalNode().id())) {
    // Local node elected leader!
  }
});
```

The topic passed as the first argument to the `run` method is an identifier for a named election in
which to participate. Multiple elections can be managed by a single leader elector, and leaders'
locations will be automatically balanced among all elections.

To get the current leader for a topic, use `getLeadership`:

```java
Leadership<NodeId> leadership = elector.getLeadership("foo");
```

The `Leadership` provided by an elector contains both a `Leader` and a list of candidates in the
election. For each unique `Leader` for a topic, a monotonically increasing unique _term_ and wall clock
time will be provided.

Finally, to listen for changes to leaderships managed by a `LeaderElector`, register a `LeadershipEventListener`:

```java
elector.addListener(event -> {
  // Either a leader or candidates changed
  Leadership leadership = event.newLeadership();
});
```

## Data primitives

Data primitives store state and can be replicated either using the partitioned Raft cluster or
a multi-primary replication protocol. These primitives are typically data structures, including:
* `AtomicCounter` - simple distributed atomic counter
* `AtomicValue` - simple distributed atomic value
* [`ConsistentMap`](#consistentmap) - partitioned `Map` with change events and support for optimistic locking
* `ConsistentTreeMap` - distributed `TreeMap` with change events and support for optimistic locking
* `ConsistentMultimap` - partitioned multimap with change events
* `DistributedSet` - partitioned `Set` with change events
* `DocumentTree` - partitioned tree-like structure with change events and support for optimistic locking

### ConsistentMap

The `ConsistentMap` primitive is modelled on Java's `Map` and provides support for listening for
changes to the map and optimistic locking. To create a map, use the `ConsistentMapBuilder`:

```java
ConsistentMap<String, String> map = atomix.consistentMapBuilder("test-map")
  .withPersistence(Persistence.EPHEMERAL)
  .withBackups(2)
  .build();

map.put("foo", "Hello world!");
```

Map values returned by the `ConsistentMap` are wrapped in a `Versioned` object which can be
used for optimistic locking:

```java
Versioned<String> value = map.get("foo");
map.replace("foo", "Hello world again!", value.version());
```

As with all other primitives, an `AsyncConsistentMap` can be created via the `async()` method:

```java
AsyncConsistentMap<String, String> asyncMap = map.async();
asyncMap.put("bar", "baz").thenRun(() -> {
  // put complete
});
```

To listen for changes to the map, add a `MapEventListener` to the map:

```java
map.addListener(event -> {
  switch (event.type()) {
    case INSERT:
      ...
    case UPDATE:
      ...
    case REMOVE:
      ...
  }
});
```

## Transactions

Atomix supports transactional operations over multiple primitives. Transactions are committed
using a two-phase commit protocol. To create a transaction, use the `TransactionBuilder`:

```java
Transaction transaction = atomix.transactionBuilder()
  .withIsolation(Isolation.REPEATABLE_READS)
  .build();
```

To begin the transaction, call `begin`:

```java
transaction.begin();
```

Once a transaction has been started, primitives can be created via the `Transaction` primitive
builders. Once all operations have been performed, call `commit()` to commit the transaction:

```java
CommitStatus status = transaction.commit();
```

The `CommitStatus` returned by the `commit()` call indicates whether the transaction was committed
successfully. Transactions may not commit successfully if a concurrent transaction is already modifying
the referenced resources.

To abort a transaction, call `abort()`:

```java
transaction.abort();
```

### TransactionalMap

To create a transactional map, use the `mapBuilder()` method:

```java
Transaction transaction = atomix.transactionBuilder()
  .withIsolation(Isolation.REPEATABLE_READS)
  .build();

transaction.begin();

TransactionalMap<String, String> transactionalMap = transaction.mapBuilder("my-map")
  .withSerializer(Serializers.using(KryoNamespaces.BASIC))
  .build();
transactionalMap.put("foo", "bar");
transactionalMap.put("bar", "baz");

if (transaction.commit() == CommitStatus.SUCCESS) {
  ...
}
```

The map will inherit the isolation level configured for the transaction. Changes to the named
map will be reflected in non-transactional maps of the same name upon commit.

### TransactionalSet

To create a transactional set, use the `setBuilder()` method:

```java
Transaction transaction = atomix.transactionBuilder()
  .withIsolation(Isolation.REPEATABLE_READS)
  .build();

transaction.begin();

TransactionalSet<String> transactionalSet = transaction.setBuilder("my-set")
  .withSerializer(Serializers.using(KryoNamespaces.BASIC))
  .build();
transactionalSet.add("foo");
transactionalSet.add("bar");

if (transaction.commit() == CommitStatus.SUCCESS) {
  ...
}
```

The set will inherit the isolation level configured for the transaction. Changes to the named
set will be reflected in non-transactional set of the same name upon commit.

### Serialization

All data primitives support custom serialization via a `Serializer` provided to the builder:

```java
Serializer dataSerializer = new Serializer() {
  @Override
  public <T> byte[] encode(T object) {
    return ...;
  }

  @Override
  public <T> T decode(byte[] bytes) {
    return ...;
  }
};

ConsistentMap<String, Data> map = atomix.consistentMapBuilder("data")
  .withSerializer(dataSerializer)
  .build();
```

Atomix also provides a core [Kryo](https://github.com/EsotericSoftware/kryo) based serialization
abstraction:

```java
Serializer dataSerializer = Serializer.using(KryoNamespace.builder()
  .register(KryoNamespaces.BASIC)
  .register(Data.class)
  .build());

ConsistentMap<String, Data> map = atomix.consistentMapBuilder("data")
  .withSerializer(dataSerializer)
  .build();
```

By default, the `KryoNamespace` uses Kryo's `FieldSerializer`, but custom Kryo serializers may
be provided via the builder. Additionally, a set of default serializers can be registered using
the `KryoNamespaces.BASIC` constant.

## REST API

The Atomix _agent_ is a standalone server or client node with a REST API through which Java or
HTTP clients can manage Atomix primitives remotely.

### Running a standalone Atomix agent

The `atomix` script is used to run the Atomix agent and CLI:

```
mvn clean package
bin/atomix agent
```

### Bootstrapping a standalone cluster

To start a cluster, the `atomix agent` command must be provided with a local node name, host, and port and
a set of nodes with which to bootstrap the cluster.

The format of addresses passed to the agent command is:

```
{name}:{host}:{port}
```

Node names must be unique among all nodes in the cluster and will default to the local host name
if not explicitly specified.

```
bin/atomix agent a:localhost:5000 --bootstrap a:localhost:5000 b:localhost:5001 c:localhost:5002 --http-port 6000 --data-dir data/a
```

```
bin/atomix agent b:localhost:5001 --bootstrap a:localhost:5000 b:localhost:5001 c:localhost:5002 --http-port 6001 --data-dir data/b
```

```
bin/atomix agent c:localhost:5002 --bootstrap a:localhost:5000 b:localhost:5001 c:localhost:5002 --http-port 6002 --data-dir data/c
```

### Starting a client node

Client nodes provide the same HTTP API as server nodes but do not themselves store state. Client
nodes can be useful for deploying an Atomix node with which to communicate locally and which can
use more efficient binary communication when transporting requests across the network.

```
bin/atomix agent --client --bootstrap a:localhost:5000 b:localhost:5001 c:localhost:5002
```

### REST API examples

This section provides a series of example usages of primitives via the HTTP API:

#### Acquire a lock
```
curl -XPOST http://localhost:5678/v1/primitives/locks/my-lock
```

#### Release a lock
```
curl -XDELETE http://localhost:5678/v1/primitives/locks/my-lock
```

#### Set a value in a map
```
curl -XPUT http://localhost:5678/v1/primitives/maps/my-map/foo -d value="Hello world!" -H "Content-Type: text/plain"
```

#### Get a value in a map
```
curl -XGET http://localhost:5678/v1/primitives/maps/my-map/foo
```

#### Send an event
```
curl -XPOST http://localhost:5678/v1/events/something-happened -d "Something happened!" -H "Content-Type: text/plain"
```

#### Receive events
```
curl -XGET http://localhost:5678/v1/events/something-happened
```

### Docker
```
mvn clean package
docker build -t atomix .
docker run -p 5678:5678 -p 5679:5679 atomix docker:0.0.0.0
```

### Interactive CLI

Atomix provides an interactive command line interface through which the Atomix cluster and its
primitives can be managed. This tool can be useful for inspecting the state of the cluster or
for demos.

#### Starting the CLI

To run the interactive CLI, set the `ATOMIX_HOST` and `ATOMIX_PORT` environment variables and
run the `atomix` script with no arguments:

```
ATOMIX_HOST=127.0.0.1 ATOMIX_PORT=5678 bin/atomix
```

To view a list of commands supported by the CLI, type `help`

#### Print help
```
atomix> election foo leader
{
  "candidates": [
    "d718897a-e651-416c-94c3-9ddc09b2d0e5",
    "7f01be7d-514f-4561-a2ba-3ccbf3edfbb2"
  ],
  "leader": "d718897a-e651-416c-94c3-9ddc09b2d0e5"
}
```

#### Running a command
```
election foo leader
```

### Acknowledgements

Thank you to the [Open Networking Foundation][ONF] and [ONOS][ONOS]
for continued support of Atomix!

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of [YourKit Java Profiler](https://www.yourkit.com/java/profiler/)
and [YourKit .NET Profiler](https://www.yourkit.com/.net/profiler/),
innovative and intelligent tools for profiling Java and .NET applications.

![YourKit](https://www.yourkit.com/images/yklogo.png)

[Website]: http://atomix.io/atomix/
[Getting started]: http://atomix.io/atomix/docs/getting-started/
[User manual]: http://atomix.io/atomix/docs/
[Google group]: https://groups.google.com/forum/#!forum/atomixio
[Javadoc]: http://atomix.io/atomix/api/latest/
[Raft]: https://raft.github.io/
[ONF]: https://www.opennetworking.org/
[ONOS]: http://onosproject.org/
[Copycat]: https://github.com/atomix/copycat
[Cluster management]: https://github.com/atomix/atomix/tree/master/core/src/main/java/io/atomix/cluster
[Messaging]: https://github.com/atomix/atomix/tree/master/core/src/main/java/io/atomix/cluster/messaging
[Partitioning]: https://github.com/atomix/atomix/tree/master/core/src/main/java/io/atomix/partition
[Distributed systems primitives]: https://github.com/atomix/atomix/tree/master/core/src/main/java/io/atomix/primitives
[Transactions]: https://github.com/atomix/atomix/tree/master/core/src/main/java/io/atomix/transaction
[Raft protocol]: https://github.com/atomix/atomix/tree/master/protocols/raft
[Gossip protocol]: https://github.com/atomix/atomix/tree/master/protocols/gossip
[Failure detection protocol]: https://github.com/atomix/atomix/tree/master/protocols/failure-detection
[Primary-backup protocol]: https://github.com/atomix/atomix/tree/master/protocols/backup
[Time protocols]: https://github.com/atomix/atomix/tree/master/time
[Protocols]: https://github.com/atomix/atomix/tree/master/protocols
