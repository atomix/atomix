Copycat
=======

[![Build Status](https://travis-ci.org/kuujo/copycat.png)](https://travis-ci.org/kuujo/copycat)

Copycat is an extensible log-based distributed coordination framework for Java 8 built on the
[Raft consensus protocol](https://raftconsensus.github.io/).

#### [User Manual](#user-manual)
#### [Architecture](#architecture)

Copycat is a CP (consistent/partition-tolerant) oriented exercise in distributed coordination built on a consistent
replicated log. The core of Copycat is an extensible asynchronous framework that uses a mixture of
[gossip](http://en.wikipedia.org/wiki/Gossip_protocol) and the [Raft consensus protocol](https://raftconsensus.github.io/)
to provide a set of high level APIs that solve a variety of distributed systems problems including:
* [Leader election](#leader-elections)
* [Replicated state machines](#state-machines)
* [Strongly consistent state logs](#state-logs)
* [Eventually consistent event logs](#event-logs)
* [Distributed collections](#collections)
* [Distributed atomic variables](#atomic-variables)
* [Failure detection](#failure-detection)
* [Messaging framework](#messaging)
* [Remote execution](#remote-execution)

**Copycat is still undergoing heavy development. The `master` branch is the current development branch and is
still undergoing testing and is therefore not recommended for production!**

**Copycat is now in a feature freeze for the 0.5 release.** This project will be released to Maven Central once is has
been significantly tested in code and via [Jepsen](https://github.com/aphyr/jepsen). Follow the project for updates!

Additionally, documentation is currently undergoing a complete rewrite due to the refactoring of the resource builder API.
Sorry for the delay!

*Copycat requires Java 8*

User Manual
===========

1. [Introduction](#introduction)
1. [Getting started](#getting-started)
   * [Concepts and terminology](#concepts-and-terminology)
   * [The builder pattern](#the-builder-pattern)
   * [Thread safety](#thread-safety)
1. [Clusters](#clusters)
   * [Creating a cluster](#creating-a-cluster)
   * [Seed members](#seed-members)
   * [Member types](#member-types)
   * [Direct messaging](#direct-messaging)
   * [Broadcast messaging](#broadcast-messaging)
   * [Serialization](#serialization)
   * [Remote execution](#remote-execution)
1. [Resources](#resources)
   * [Creating resources](#creating-resources)
   * [Resource clusters](#resource-clusters)
   * [Protocols](#protocols)
   * [Partitions](#partitions)
   * [Replication strategies](#replication-strategies)
1. [Event logs](#event-logs)
   * [Creating an event log](#creating-an-event-log)
   * [Writing events to the event log](#writing-events-to-the-event-log)
   * [Consuming events from the event log](#consuming-events-from-the-event-log)
1. [State logs](#state-logs)
   * [Creating a state log](#creating-a-state-log)
   * [State commands](#state-commands)
   * [Submitting commands to the state log](#submitting-commands-to-the-state-log)
1. [State machines](#state-machines)
   * [Creating a state machine](#creating-a-state-machine)
   * [Designing state machine states](#designing-state-machine-states)
   * [State machine commands](#state-machine-commands)
   * [Synchronous proxies](#synchronous-proxies)
   * [Asynchronous proxies](#asynchronous-proxies)
1. [Leader elections](#leader-elections)
   * [Creating a leader election](#creating-a-leader-election)
1. [Collections](#collections)
   * [AsyncMap](#asyncmap)
   * [AsyncList](#asynclist)
   * [AsyncSet](#asyncset)
   * [AsyncMultiMap](#asyncmultimap)
   * [AsyncLock](#asynclock)
1. [Atomic variables](#atomic-variables)
   * [AsyncLong](#asynclong)
   * [AsyncBoolean](#asyncboolean)
   * [AsyncReference](#asyncreference)
1. [Architecture](#architecture)
   * [Logs](#logs)
   * [Strong consistency and Copycat's Raft consensus protocol](#strong-consistency-and-copycats-raft-consensus-protocol)
      * [Leader election](#leader-election-1)
      * [Command persistence](#command-persistence)
      * [Command consistency](#command-consistency)
      * [Log compaction](#log-compaction)
   * [Eventual consistency and Copycat's gossip protocol](#eventual-consistency-and-copycats-gossip-protocol)
      * [Passive membership](#passive-membership)
      * [Log replication](#log-replication)
1. [Contributing](#contributing)

## Introduction

Copycat is a distributed coordination framework built on the
[Raft consensus protocol](https://raftconsensus.github.io/). Copycat facilitates numerous types of strongly consistent
distributed data structures - referred to as [resources](#resources) - based on a replicated log. Each Copycat
cluster can support multiple resources, each resource may consist of multiple [partitions](#resource-partitions), and
each partition runs a separate instance of the [Raft consensus algorithm](#strong-consistency-and-copycats-raft-consensus-protocol)
to perform leader election and log replication. This allows Copycat to spread load across large clusters.

The Copycat [cluster](#the-copycat-cluster) consists of a core set of [seed members](#seed-members) which are a set
of nodes commonly known to all members of the cluster. The cluster uses a simple gossip protocol to locate new members
and detect failures in old ones.

For more information on Copycat's leader election and replication implementation see the in depth explanation of
[Copycat's architecture](#architecture).

## Getting started

The Copycat project is structured as several Maven submodules.

Copycat provides a single module - `copycat-all` which aggregates all of the standard Copycat modules including the
[Netty](http://netty.io) cluster and Raft consensus protocol.

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-all</artifactId>
  <version>0.5.0-SNAPSHOT</version>
</dependency>
```

Additionally, the following resource modules are provided:
* `copycat-event-log` - A replicated log designed for recording event histories
* `copycat-state-log` - A replicated log designed for strongly consistent recording of state machine
  command histories
* `copycat-state-machine` - A replicated state machine framework built on top of `copycat-state-log` using proxies
* `copycat-leader-election` - A simple leader election framework built on top of `StateLog`
* `copycat-collections` - A set of strongly consistent distributed collection APIs built on top of `StateMachine`
* `copycat-atomic` - A set of asynchronous distributed atomic variables build on top of `StateMachine`

### Concepts and terminology

Copycat is a fairly large project with many moving parts. It's important that users understand some of the concepts and
terminology used throughout this documentation.

* **Resource** - a high-level user API for operating on distributed data structures such as maps, lists, logs, and other
  state machines.
* **Cluster** - the interface through which instances of Copycat's resources communicate with one another
* **Member** - an interface to a single node in a cluster
* **Seed members** - a consistent set of cluster members commonly know to all nodes in a cluster
* **Raft** - a strongly consistent distributed consensus protocol on which Copycat's log replication is built
* **Gossip** - a lightweight communication protocol for sharing information between nodes in a cluster

### The builder pattern

Copycat makes heavy use of the [builder pattern](http://en.wikipedia.org/wiki/Builder_pattern) to afford users fine
grained control over the configuration of immutable resources and their relationships with each other. All user facing
APIs in Copycat are facilitated by associated builders.

Copycat builders can normally be constructed via a static `builder()` factory method on their associated class or interface.
Builder methods are normally prefixed by `with` rather than `set`. To construct an object, the `Builder` interface
exposes a `build()` method common to all Copycat builders.
For example:

```java
Cluster cluster = NettyCluster.builder()
  .withMemberId(1)
  .withSeed(NettyMember.builder()
    .withId(1)
    .withHost("localhost")
    .withPort(1234)
    .build())
  .build();
```

#### Thread safety

All of Copycat's user-facing APIs are thread safe.

## Clusters

The `Cluster` API is the interface through which all of Copycat's resources communicate with one another. The default
`Cluster` implementation - `NettyCluster` - uses a gossip protocol to perform membership and failure detection. Given
a set of seed members (members commonly known to all nodes in the cluster), nodes connect to the seed members and
retrieve membership information regarding other members of the cluster.

### Creating a cluster

Copycat provides a default [Netty](http://netty.io) based cluster implementation via the `copycat-netty` project. To
use the Netty cluster, add the `copycat-netty` dependency to your project:

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-netty</artifactId>
  <version>0.5.0-SNAPSHOT</version>
</dependency>
```

`Cluster` instances are constructed via the [builder pattern](http://en.wikipedia.org/wiki/Builder_pattern) which is
common throughout all of Copycat's APIs. This patterns affords users fine grained control over the relationships
between resources.

```java
NettyCluster cluster = NettyCluster.builder()
  .withMemberId(1)
  .addSeed(NettyMember.builder()
    .withId(1)
    .withHost("localhost")
    .withPort(8080)
    .build())
  .addSeed(NettyMember.builder()
    .withId(2)
    .withHost("localhost")
    .withPort(8081)
    .build())
  .addSeed(NettyMember.builder()
    .withId(3)
    .withHost("localhost")
    .withPort(8082)
    .build())
  .build();
```

Each `Cluster` instance is comprised of a single local member (a server) and any number of remote members. Each cluster
must be configured with an initial set of [seed members](#seed-members). Copycat will use the seed members to locate
other non-seed members in the cluster automatically. Each member is defined with a unique `id` that *must be identical
across physical servers.* The seed member at `localhost:8080` is defined as member `1`, and it should be defined as a
seed member `1` on all other nodes in the cluster. This is the mechanism by which Copycat associates members with one another.

### Seed members

Each `Cluster` instance must consist of some set of *seed members*. Seed members are a fixed set of members commonly
known to the entire cluster. Given a set of seed members, the `Cluster` will connect to each member to retrieve a full
membership list. Additionally, all members will periodically communicate with one another throughout the lifetime of
the cluster in order to detect new members and remove failed members.

Seed members are not just critical to Copycat's clustering for membership detection. Seed members are also the only members
of the cluster that are allowed to fully participate in the Raft consensus protocol as voting members.

Seed members must be explicitly defined in the cluster builder via the `addSeed` method.

```java
NettyCluster cluster = NettyCluster.builder()
  .withMemberId(6)
  .withMemberType(Member.Type.PASSIVE)
  .addSeed(NettyMember.builder()
    .withId(1)
    .withHost("localhost")
    .withPort(8080)
    .build())
  .addSeed(NettyMember.builder()
    .withId(2)
    .withHost("localhost")
    .withPort(8081)
    .build())
  .addSeed(NettyMember.builder()
    .withId(3)
    .withHost("localhost")
    .withPort(8082)
    .build())
  .build();
```

Each seed member's `id` *must be consistent across all nodes in the cluster* to ensure smooth operation. Note that
no additional members need to be added to the cluster since they will be detected once the cluster is started.

### Member types

In addition to seed members, Copycat also supports a couple of other member types. Member types can be specified in the
`Cluster` builder:

```java
NettyCluster cluster = NettyCluster.builder()
  .withMemberId(6)
  .withMemberType(Member.Type.PASSIVE)
  ...
```

#### Active members

The `ACTIVE` member type is a synonym for seed members. From the perspective of the Raft protocol, active members are
the only members that are allowed to be full voting members in a Raft cluster.

#### Passive members

The `PASSIVE` member type is used to indicate that a member is a participant in resource replication, but not in the
Raft consensus protocol itself. Specifically, when using the `RaftProtocol`, passive members will receive replicated
state via a gossip protocol. This means that passive members represent an eventually consistent model.

#### Remote members

The `REMOTE` member type is used to indicate that a member does not participate in state replication in any way. Passive
members amount to nothing more than elaborate clients. Logs will never be replicated on to a passive member's machine,
and therefore passive members must always make remote requests to receive state information.

### Direct messaging

Copycat uses a simple messaging framework to communicate between nodes in order to perform coordination, leader election,
and replication, and that framework is exposed to the user for each resource as well. Copycat's messaging framework
uses topics to route messages to specific message handlers on any given node.

To register a handler to receive messages for a topic, call the `registerHandler` method on the `LocalMember` instance.

```java
copycat.stateMachine("my-state-machine").open().thenAccept(stateMachine -> {
  stateMachine.cluster().member().registerHandler(message -> {
    return CompletableFuture.completedFuture("world!");
  });
});
```

Cluster message handlers must return a `CompletableFuture`. If the message is handled synchronously, simply return
an immediately completed future as demonstrated above. Otherwise, asynchronous calls to Copycat resources can be made
within the message handler without blocking the Copycat event loop.

To send a message to another member in the cluster, call the `send` method on any `Member` instance.

```java
copycat.stateMachine("my-state-machine").open().thenAccept(stateMachine -> {
  stateMachine.cluster().members().forEach(member -> {
    member.send("Hello").thenAccept(reply -> {
      System.out.println("Hello " + reply); // Hello world!
    });
  });
});
```

### Broadcast messaging

Copycat also allows the exposed `Cluster` to be used to broadcast messages to all members of the cluster. Note that
Copycat's broadcast functionality is *not* guaranteed, and Copycat makes no promises about either the order in which
messages will be delivered or even whether they'll be delivered at all.

Cluster-wide broadcast messages work similarly to direct messages in that they are topic based. To register a broadcast
message listener call `addBroadcastListener` on any `Cluster` instance:

```java
copycat.cluster().addBroadcastListener(message -> {
  System.out.println("Got message " + message);
});
```

To broadcast a message to all members of a cluster use the `broadcast` method on the `Cluster` instance:

```java
copycat.cluster().broadcast("Hello world!");
```

Note that broadcasts apply *only to the membership for the cluster on which the message was broadcast*. In other words,
broadcast messages will only be sent to `ALIVE` members of the cluster through which the message was sent. If you want
to broadcast a message to *all* of the members of the Copycat cluster, use the global `Cluster` which can be retrieved
through the `Copycat` instance.

### Serialization

Copycat provides a fast custom serialization API that allows it to significantly optimize performance by reducing
garbage collection and using off-heap memory. Users can create Copycat serializable types by simply implementing the
`Writable` interface:

```java
public class Person implements Writable {
  private String firstName;
  private String lastName;

  public Person() {
  }

  public Person(String firstName, String lastName) {
    this.firstName = firstName;
    this.lastName = lastName;
  }

  @Override
  public void writeObject(Buffer buffer) {
    buffer.writeInt(firstName.getBytes().length).write(firstName.getBytes());
    buffer.writeInt(lastName.getBytes().length).write(lastName.getBytes());
  }

  @Override
  public void readObject(Buffer buffer) {
    byte[] firstNameBytes = new byte[buffer.readInt()];
    buffer.read(firstNameBytes);
    firstName = new String(firstNameBytes);
    byte[] lastNameBytes = new byte[buffer.readInt()];
    buffer.read(lastNameBytes);
    firstName = new String(lastNameBytes);
  }

}
```

### Remote execution

In addition to sending messages to other members of the cluster, Copycat also supports execution of arbitrary tasks on
remote nodes. For example, this is particularly useful for executing code on a resource's leader node once a leader has
been elected.

```java
Task<String> sayHello = () -> "Hello world!";

copycat.stateMachine("my-state-machine").open().thenAccept(stateMachine -> {
  stateMachine.cluster().election().addListener(result -> {
    result.winner().submit(sayHello).thenAccept(result -> {
      System.out.println(result); // Hello world!
    });
  });
});
```

## Resources

Resources are the high-level user data structures that wrap Copycat's underlying communication and replication
framework. *Resource* is simply an abstract term for all of the log-based data types provided by Copycat. Ultimately,
each resource whether it be an [event log](#event-logs), [state log](#state-logs), [state machine](#state-machines), or
[collection](#collections) - is managed by a [cluster](#clusters) and backed by a Raft replicated [log](#logs).

### Creating resources

Each of Copycat's resource implementations are modularized and made available as a separate Maven project. In addition
to adding the resource project as a dependency, users must also add the necessary [protocol](#resource-protocols)
and [cluster](#clusters) as dependencies.

For instance, the following Maven dependency list adds `copycat-collections`, `copycat-raft`, and `copycat-netty` as
dependencies in order to construct an [AsyncMap](#asyncmap).

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-collections</artifactId>
  <version>0.5.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-raft</artifactId>
  <version>0.5.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-netty</artifactId>
  <version>0.5.0-SNAPSHOT</version>
</dependency>
```

As with all Copycat APIs, resources are commonly constructed via the [builder pattern](#the-builder-pattern).

```java
AsyncMap.Builder builder = AsyncMap.builder();
```

### Resource clusters

In order to communicate, each resource requires a `Cluster` instance. The cluster can be configured by simply setting
the cluster on the respective resource's builder:

```java
AsyncMap.Builder builder = AsyncMap.builder()
  .withCluster(NettyCluster.builder()
    .withMemberId(1)
    .addSeed(NettyMember.builder()
      .withId(1)
      .withHost("localhost")
      .withPort(8080)
      .build())
    .addSeed(NettyMember.builder()
      .withId(2)
      .withHost("localhost")
      .withPort(8081)
      .build())
    .addSeed(NettyMember.builder()
      .withId(3)
      .withHost("localhost")
      .withPort(8082)
      .build())
    .build());
```

See the [cluster documentation](#cluster) for more information on configuring the cluster.

### Protocols

Copycat's resource abstract is designed with no inherent knowledge of the underlying replication protocol. This allows
users to theoretically change the protocol used for a given resource, but it also means the user must specify the protocol
instance as well.

Currently, Copycat's sole `Protocol` implementation is the `RaftProtocol`. To configure the Raft consensus protocol on
a resource, use the `RaftProtocol.Builder`:

```java
AsyncMap.Builder builder = AsyncMap.builder()
  .withLog(DiscreteStateLog.builder()
    .withCluster(NettyCluster.builder()
      .withMemberId(1)
      .addSeed(NettyMember.builder()
        .withId(1)
        .withHost("localhost")
        .withPort(8080)
        .build())
      .addSeed(NettyMember.builder()
        .withId(2)
        .withHost("localhost")
        .withPort(8081)
        .build())
      .addSeed(NettyMember.builder()
        .withId(3)
        .withHost("localhost")
        .withPort(8082)
        .build())
      .build())
    .withProtocol(RaftProtocol.builder()
      .withStorage(BufferedStorage.builder()
        .withName("my-resource")
        .withDirectory("logs/my-resource")
        .build())
      .build());
    .build());
```

This example demonstrates the relationship between resources and logs. As with other collections and atomic variables,
the `AsyncMap` resource is a thin layer over Copycat's distributed [state log](#state-logs). The state log communicates
with other resources of the same type via the provided `Cluster`.

### Partitions

In addition to single resource clusters, Copycat supports partitioned (or sharded) resources for higher throughput.
Both of Copycat's low-level logs ([event log](#event-logs) and [state log](#state-logs)) support partitioning via
`PartitionedEventLog` and `PartitionedStateLog` respectively.

Partitioned resources are constructed using the builder pattern; the only difference is that partitioned resources
consist of one or many `Partition` instances.

```java
AsyncMap<String, String> map = AsyncMap.<String, String>builder()
  .withLog(PartitionedStateLog.builder()
    .withName("my-map")
    .withCluster(TestCluster.builder()
      .withMemberId(1)
      .addSeed(TestMember.builder()
        .withId(1)
        .withAddress("map-1")
        .build())
      .addSeed(TestMember.builder()
        .withId(2)
        .withAddress("map-2")
        .build())
      .addSeed(TestMember.builder()
        .withId(3)
        .withAddress("map-3")
        .build())
      .withRegistry(registry)
      .build())
    .withPartitioner(new HashPartitioner())
    .addPartition(StateLogPartition.builder()
      .withProtocol(RaftProtocol.builder()
        .withStorage(BufferedStorage.builder()
          .withName("map-partition-0")
          .withDirectory("logs/map/partition-0")
          .build())
        .build())
      .withReplicationStrategy(new PartitionedReplicationStrategy(3))
      .build())
    .addPartition(StateLogPartition.builder()
      .withProtocol(RaftProtocol.builder()
        .withStorage(BufferedStorage.builder()
          .withName("map-partition-1")
          .withDirectory("logs/map/partition-1"))
          .build())
        .build())
      .withReplicationStrategy(new PartitionedReplicationStrategy(3))
      .build())
    .addPartition(StateLogPartition.builder()
      .withProtocol(RaftProtocol.builder()
        .withStorage(BufferedStorage.builder()
          .withName("map-partition-2")
          .withDirectory("logs/map/partition-2")
          .build())
        .build())
      .withReplicationStrategy(new PartitionedReplicationStrategy(3))
      .build())
    .build())
  .build();
```

When a log resource is partitioned, Copycat will replicate the log *on a subset of the cluster* according to the provided

### Replication strategies

## State machines

The most common use case for consensus algorithms is to support a strongly consistent replicated state machine.
Replicated state machines are similarly an essential feature of Copycat. Copycat provides a unique, proxy based
interface that allows users to operate on state machines either synchronously or asynchronously.

### Creating a state machine

To create a replicated state machine, you first must design an interface for the state machine states. In this case
we'll use one of Copycat's provided [collections](#collections) as an example.

States are used internally by Copycat to apply state machine commands and queries and transition between multiple
logical states. Each state within a state machine must implement a consistent explicitly defined interface. In this
case we're using the existing Java `Map` interface to define the state.

```java
public class MapState<K, V> implements Map<K, V> {
  private Map<K, V> map;

  @Override
  @Write
  public V put(K key, V value) {
    return map.put(key, value);
  }

  @Override
  @Read
  public V get(K key) {
    return map.get(key);
  }

  @Override
  @Delete
  public V remove(K key) {
    return map.remove(key);
  }

  @Override
  @Read
  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

}
```

Once we've defined the map state, we can simply create a new map state machine via the `StateMachine` builder.

```java
StateMachine<MapState> stateMachine = StateMachine.builder()
  .withLog(PartitionedStateLog.builder()
    .withName("my-map")
    .withCluster(NettyCluster.builder()
      .withMemberId(1)
      .addSeed(NettyMember.builder()
        .withId(1)
        .withHost("localhost")
        .withPort(8080)
        .build())
      .addSeed(NettyMember.builder()
        .withId(2)
        .withHost("localhost")
        .withPort(8081)
        .build())
      .addSeed(NettyMember.builder()
        .withId(3)
        .withHost("localhost")
        .withPort(8082)
        .build())
      .build())
    .withPartitioner(new HashPartitioner())
    .addPartition(StateLogPartition.builder()
      .withProtocol(RaftProtocol.builder()
        .withStorage(BufferedStorage.builder()
          .withName("partition-0")
          .withDirectory("logs/partition-0")
          .build())
        .build())
      .withReplicationStrategy(new PartitionedReplicationStrategy(3))
    .build())
    .addPartition(StateLogPartition.builder()
      .withProtocol(RaftProtocol.builder()
        .withStorage(BufferedStorage.builder()
          .withName("partition-1")
          .withDirectory("logs/partition-1")
          .build())
        .build())
      .withReplicationStrategy(new PartitionedReplicationStrategy(3))
    .build())
    .addPartition(StateLogPartition.builder()
      .withProtocol(RaftProtocol.builder()
        .withStorage(BufferedStorage.builder()
          .withName("partition-2")
          .withDirectory("logs/partition-2")
          .build())
        .build())
      .withReplicationStrategy(new PartitionedReplicationStrategy(3))
    .build())
  .build());

stateMachine.open().thenRun(() -> {
  System.out.println("State machine opened!");
});
```

When a Copycat resource - such as a state machine, log, or election - is created, the resource must be opened before
it can be used. Practically all Copycat interfaces return `CompletableFuture` instances which can either be used to
buffer until the result is received or receive the result asynchronously. In this case we just buffer by calling the
`get()` method on the `CompletableFuture`.

### Designing state machine states

State machines consist of a series of states and transitions between them. Backed by a state log, Copycat guarantees
that commands (state method calls) will be applied to the state machine in the same order on all nodes of a Copycat
cluster. Given the same commands in the same order, state machines should always arrive at the same state with the
same output (return value). That means all commands (methods) of a state machine state *must be deterministic*; commands
cannot depend on the state of shared external systems such as a database or file system.

State machines are defined by simply defining an interface. While a state machine can consist of many states (state
interface implementations), all states must implement the same interface. This allows Copycat to expose a consistent
interface regardless of the current state of the state machine.

State machine states do not have to extend any particular interface, nor do they have to implement any interface at all.

### State machine commands

State machine commands are annotated methods on the state machine state interface. Annotating a method with the
`@Write` annotation indicates that it is a stateful method call, meaning the operation that calls the method should
be persisted to the log and replicated. It is vital that *all* methods which alter the state machine's state be
identified by the `@Write` annotation. For this reason, all state machine methods are commands by default.

```java
public interface MapState<K, V> {

  @Write
  V put(K key);

  @Read
  V get(K key);

  @Delete
  V remove(K key);

}
```

### Synchronous proxies

While Copycat's `StateMachine` interface exposes a `submit` method for submitting named commands and queries to the
state machine, it also provides a proxy based interface for operating on the state machine state interface directly.
To create a state machine proxy simply call the `createProxy` method on the `StateMachine` object, passing the proxy
interface to implement. The state machine supports both synchronous and asynchronous return values by checking whether
a given proxy interface method's return type is `CompletableFuture`.

```java
Map<String, String> proxy = stateMachine.createProxy(Map.class);
```

Once we've created a state machine proxy, we can simply call methods directly on the proxy object and Copycat will
internally submit [commands](#commands) and [queries](#queries) to the replicated state machine.

```java
proxy.put("foo", "Hello world!");
System.out.println(proxy.get("foo")); // Hello world!
```

### Asynchronous proxies

The state machine proxy feature also supports asynchronous proxies. It does this simply by checking the return type
of a given proxy method. If the proxy method's return type is `CompletableFuture` then the method will be executed
in a separate thread.

Often times asynchronous proxies require creating a separate proxy interface.

```java
public interface AsyncMap<K, V> {

  CompletableFuture<V> put(K key, V value);

  CompletableFuture<V> get(K key);

  CompletableFuture<V> remove(K key);

  CompletableFuture<Void> clear();

}
```

Aside from the return types, asynchronous proxies work the same way as synchronous proxies. To create an asynchronous
proxy simply call the `createProxy` method, passing the asynchronous proxy interface.

```java
AsyncMap<K, V> map = stateMachine.createProxy(AsyncMap.class);

map.put("foo", "Hello world!").thenRun(() -> {
  map.get("foo").thenAccept(value -> System.out.println(value)); // Hello world!
});
```

## Event logs

Event logs are eventually consistent Raft replicated logs that are designed to be compacted based on time or size.

### Creating an event log

To create an event log, user the event log `Builder`:

```java
EventLog<String, String> eventLog = PartitionedEventLog.builder()
  .withName("my-map")
  .withCluster(NettyCluster.builder()
    .withMemberId(1)
    .addSeed(NettyMember.builder()
      .withId(1)
      .withHost("localhost")
      .withPort(8080)
      .build())
    .addSeed(NettyMember.builder()
      .withId(2)
      .withHost("localhost")
      .withPort(8081)
      .build())
    .addSeed(NettyMember.builder()
      .withId(3)
      .withHost("localhost")
      .withPort(8082)
      .build())
    .build())
  .withPartitioner(new HashPartitioner())
  .addPartition(StateLogPartition.builder()
    .withProtocol(RaftProtocol.builder()
      .withStorage(BufferedStorage.builder()
        .withName("partition-0")
        .withDirectory("logs/partition-0")
        .build())
      .build())
    .withReplicationStrategy(new PartitionedReplicationStrategy(3))
  .build())
  .addPartition(StateLogPartition.builder()
    .withProtocol(RaftProtocol.builder()
      .withStorage(BufferedStorage.builder()
        .withName("partition-1")
        .withDirectory("logs/partition-1")
        .build())
      .build())
    .withReplicationStrategy(new PartitionedReplicationStrategy(3))
  .build())
  .addPartition(StateLogPartition.builder()
    .withProtocol(RaftProtocol.builder()
      .withStorage(BufferedStorage.builder()
        .withName("partition-2")
        .withDirectory("logs/partition-2")
        .build())
      .build())
    .withReplicationStrategy(new PartitionedReplicationStrategy(3))
  .build());

eventLog.open().thenRun(() -> {
  System.out.println("Event log opened!");
});
```

### Writing events to the event log

All Copycat logs are key-based. It's important to note that entries with the same key will eventually be compacted from
the log. This means that *only the last entry with a given key will remain in the log*.

To write events to an event log, simply call the `commit(K key, T entry)` method:

```java
eventLog.commit("foo", "Hello world!");
```

The `CompletableFuture` returned by the `commit` method will be completed once the entry has been logged and replicated
to a majority of the resource's replica set.

### Consuming events from the event log

To consume messages from the event log, register a message consumer via the `consumer` method:

```java
eventLog.consumer(entry -> System.out.println(entry));
```

## State logs

State logs are strongly consistent Raft replicated logs designed for persisting state. These logs are the basis of
Copycat's state machine and the data structures built on top of it.

### Creating a state log

To create a state log, use the state log `Builder`

```java
StateLog<String, String> eventLog = PartitionedStateLog.builder()
  .withName("my-map")
  .withCluster(NettyCluster.builder()
    .withMemberId(1)
    .addSeed(NettyMember.builder()
      .withId(1)
      .withHost("localhost")
      .withPort(8080)
      .build())
    .addSeed(NettyMember.builder()
      .withId(2)
      .withHost("localhost")
      .withPort(8081)
      .build())
    .addSeed(NettyMember.builder()
      .withId(3)
      .withHost("localhost")
      .withPort(8082)
      .build())
    .build())
  .withPartitioner(new HashPartitioner())
  .addPartition(StateLogPartition.builder()
    .withProtocol(RaftProtocol.builder()
      .withStorage(BufferedStorage.builder()
        .withName("partition-0")
        .withDirectory("logs/partition-0")
        .build())
      .build())
    .withReplicationStrategy(new PartitionedReplicationStrategy(3))
  .build())
  .addPartition(StateLogPartition.builder()
    .withProtocol(RaftProtocol.builder()
      .withStorage(BufferedStorage.builder()
        .withName("partition-1")
        .withDirectory("logs/partition-1")
        .build())
      .build())
    .withReplicationStrategy(new PartitionedReplicationStrategy(3))
  .build())
  .addPartition(StateLogPartition.builder()
    .withProtocol(RaftProtocol.builder()
      .withStorage(BufferedStorage.builder()
        .withName("partition-2")
        .withDirectory("logs/partition-2")
        .build())
      .build())
    .withReplicationStrategy(new PartitionedReplicationStrategy(3))
  .build());

stateLog.open().thenRun(() -> {
  System.out.println("State log opened!");
});
```

### State commands

State logs work to alter state by applying log entries to registered state commands. Commands are operations which
alter the state machine state. When submitted to the state log, all commands (writes) go through the resource leader
and are logged and replicated prior to being applied to the command function.

To register a state command, use the builder's `addCommand` method.

```java
builder.addCommand("put", (key, entry) -> data.put(key, entry));
```

### Submitting commands to the state log

Once [commands](#state-commands) have been registered on the log, commands can be submitted to the log. To submit a
named command to the log, call the `submit` method.

```java
stateLog.submit("put", "foo", "Hello world!");
```

All Copycat logs are key-based. It's important to note that entries with the same key will eventually be compacted from
the log. This means that *only the last entry with a given key will remain in the log*.

The `CompletableFuture` returned by the `submit` method will be completed once the entry has been logged and replicated
on a majority of the replicas for the partition for the given key.

## Leader elections

Leader elections are perhaps the simplest feature of Copycat, but often one of the most useful as well. One of the more
significant challenges faced by many distributed systems is coordinating leadership among a set of nodes. Copycat's
simple Raft based leader election implementation allows users to elect a leader within a resource cluster and then
use Copycat's messaging and remote execution features to react on that leader election.

Note that the leader election resource type simply wraps the resource `Cluster` based `LeaderElection` API. In reality,
all Copycat resources participate in elections, and resource elections can be accessed via each respective resource's
`Cluster`.

### Creating a leader election

To create a `LeaderElection` user the `Builder` API.

The leader election will trigger an event when the local member is elected leader for the resource:

```java
election.addListener(event -> {
  System.out.println("I was elected leader");
});
election.open();
```

## Collections

Copycat provides a variety of strongly consistent distributed data structures built on top of its
[state machine](#state-machines) framework.
* [AsyncMap](#asyncmap)
* [AsyncList](#asynclist)
* [AsyncSet](#asyncset)
* [AsyncMultiMap](#asyncmultimap)

### AsyncMap

*TODO*

### AsyncList

*TODO*

### AsyncSet

*TODO*

### AsyncMultiMap

*TODO*

## Atomic variables

Copycat also provides a variety of atomic wrappers on top of its state machine framework.
* [AsyncLong](#asynclong)
* [AsyncBoolean](#asyncboolean)
* [AsyncReference](#asyncreference)

### AsyncLong

*TODO*

### AsyncBoolean

*TODO*

### AsyncReference

*TODO*

## Architecture

Much of the preceding documentation has revolved around the features of Copycat and how to use them.
However, many users and potential contributors may want to get a better understanding of how Copycat works. This section
will detail precisely how Copycat performs tasks like leader election and log replication via the
[Raft consensus protocol](https://raftconsensus.github.io/) and dynamic membership and failure detection via
[gossip](http://en.wikipedia.org/wiki/Gossip_protocol).

### Logs

Logs are the center of the universe in the Copycat world. The global Copycat cluster and each of the resources contained
within it are all ultimately built on a consistent, Raft replicated log. Because logs are such a central component to
Copycat, much development effort has gone into the design and implementation of each of Copycat's logs.

Copycat's logs are designed with three features in mind:
* Fast writes
* Fast reads
* Fast compaction

In order to support these three vital functions, Copycat's logs are broken into segments. Each log may consist of
several segments of a configurable size, and each segment typically has a related index. The index is a simple list
of 8-bit pairs of integers indicating the entry index and position within a given segment. This allows Copycat to
quickly locate and read entries from the log.

Additionally, each log implementation supports [log compaction](#log-compaction) on an arbitrary index.

### Strong consistency and Copycat's Raft consensus protocol

Copycat uses a unique, extensible implementation of the
[Raft distributed consensus algorithm](https://raftconsensus.github.io/) to provide strong consistency guarantees for
resource management, leader elections, and the log replication which underlies all of Copycat's resources.

Raft is the simpler implementation of only a few provable algorithms for achieving consensus in a distributed system
(the other more common implementation is [Paxos](http://en.wikipedia.org/wiki/Paxos_%28computer_science%29)).
Designed for understandability, Raft uses a mixture of RPCs and timeouts to orchestrate leader election and replication
of a distributed log. In terms of the [CAP theorem](http://en.wikipedia.org/wiki/CAP_theorem), Raft - and by
extension - Copycat is a CP system, meaning in the face of a partition, Raft and Copycat favor consistency over
availability. This makes Raft a perfect fit for storing small amounts of mission critical state.

#### Leader election

In Raft, all writes are required to go through a cluster leader. Because Raft is designed to tolerate failures, this
means the algorithm must be designed to elect a leader when an existing leader's node crashes.

In Copycat, since each resource maintains its own replicated log, leader elections are performed among the
[active member](#active-member) replicas for each resource in the cluster. This means at any given point in time,
leaders for various resources could reside on different members of the cluster. However,
[passive members](#passive-members) cannot be elected leader as they do not participate in the Raft consensus algorithm.
For instance, in a cluster with three active members and ten passive members, the leader for an event log
could be active member *A*, while a state machine's leader could be active member *C*, but neither resource's leader
could be any passive member of the cluster. Copycat's `Cluster` and its related Raft implementation are designed to act
completely independently of other `Cluster` and Raft instances within the same Copycat instance.

In order to ensure consistency, leader election in Copycat is performed largely by comparing the logs of different
members of the cluster. When a member of the Copycat cluster stops receiving heartbeats from the cluster's leader, the
member transitions into the `CANDIDATE` state and begins a new election. Elections consist of sending a `PollRequest`
to each member of the cluster requesting a vote. Upon receiving a poll request, each member will compare information
about the candidate's log with its own log and either accept or reject the candidate's request based on that
information. If the candidate's log as as up-to-date or more recent than the polled member's log, the polled member
will accept the vote request. Otherwise, the polled member will reject the vote request. Once the candidate has received
successful votes from a majority of the cluster (including itself) it transitions to `LEADER` and begins accepting
requests and replicating the resource log.

Note that since Raft requires a leader to perform writes, during the leader election period the resource will be
unavailable. However, the period of time between a node crashing and a new leader being elected is typically very
small - on the order of milliseconds or seconds.

The consistency checks present in the Raft consensus algorithm ensure that the member with the most up-to-date log in
the cluster will be elected leader. In other words, through various restrictions on how logs are replicated and how
leaders are elected, Raft guarantees that *no member with an out of date log will be elected leader*, ensuring that once
entries are committed to the Copycat cluster, they are guaranteed to remain in the log until removed either via
compaction or deletion.

#### Write replication

Once a leader is elected via the Raft consensus protocol, the Copycat cluster can begin accepting new entries for the
log. All writes to any Copycat resource are *always* directed through the resource's cluster leader, and writes are
replicated from the leader to followers. This allows writes to the resource log to be coordinated from a single
location.

Commands - write operations - can be submitted to any member of the Copycat cluster. If a command is submitted to a
member that is not the leader, the command will be forwarded to the cluster leader. When the leader receives a command,
it immediately logs the entry to its local persistent log. Once the entry has been logged the leader attempts to
synchronously replicate the entry to a majority of the [active members](#active-members) of the cluster. Once a
majority of the cluster has received the entry and written it to its log, the leader will apply the entry to its state
machine and reply with the result.

Note that writes to a Copycat resource are *not* synchronously replicated to [passive members](#passive-members).
Passive members receive only successfully committed log entries via a
[gossip protocol](#eventual-consistency-and-copycats-gossip-protocol).

Logging commands to a replicated log allows Copycat to recover from failures by simply replaying log entries. For
instance, when a state machine resource crashes and restarts, Copycat will automatically replay all of the entries
written to that state machine's log in order to rebuild the state machine state. Because all logs are built from
writes to a single leader, and because Raft guarantees that entries are replicated on a majority of the servers
prior to being applied to the state machine, all state machines within the Copycat cluster are guaranteed to receive
commands in the same order.

#### Read consistency

While Raft dictates that writes to the replicated log go through the resource's leader, reads allow for slightly more
flexibility depending on the specific use case. Copycat supports several read consistency modes.

In order to achieve strong consistency in any Raft cluster, though, queries - read operations - must go through the
cluster leader. This is because, as with any distributed system, there is always some period of time before full
consistency can be achieved. Once a leader commits a write to the replicated log and applies the entry to its state
machine, it still needs to notify followers that the entry has been committed. During that period of time, querying the
leader's state will result in fully consistent state, but reading state from followers will result in stale output.

Without additional measures, though, reading directly from the leader still does not guarantee consistency. Information
only travels at the speed of light, after all. What if the leader being queried is no longer the leader? Raft and
Copycat guard against this scenario by polling a majority of the active members of the cluster to ensure that the
leader is still who he thinks he is.

This all may sound very inefficient for reads, and it is. That's why Copycat provides some options for making
trade-offs. Copycat provides three different `Consistency` modes:
* `STRICT` - Queries go through the cluster leader. When handling a query request, the leader performs a leadership
  consistency check by polling a majority of the cluster. If any member of the cluster known of a leader with a higher
  term, the member will respond to the consistency check notifying the queried leader of the new leader, and the
  queried leader will ultimately step down. This consistency mode guarantees strong consistency for all queries
* `LEASE` - Queries go through the cluster leader, but the leader performs consistency checks only periodically. For
  most requests, this mode provides strong consistency. Because the default consistency mode uses leader lease
  timeouts that are less than the cluster's election timeout, only a unique set of circumstances could result in
  inconsistency when this mode is used. Specifically, the leader would theoretically need to be blocked due to log
  compaction on the most recent log segment (indicating misconfiguration) or some other long-running process while
  simultaneously a network partition occurs, another leader is elected, and the new leader accepts writes. That's a
  pretty crazy set of circumstances if you ask me :-)
* `EVENTUAL` - Queries can be performed on any member of the cluster. When a member receives a query request, if the query's
  consistency mode is `WEAK` then the query will be immediately applied to that member's state machine. This rule
  also applies for [passive members](#passive-members).

While this functionality is not exposed by all resource APIs externally, Copycat allows read consistency to be specified
on a per-request basis via the `CommandRequest`.

#### Log compaction

One of the issues with commit logs is their every-growing nature. In the event of a failure, in order to rebuild state
for in-memory state machines, Copycat must replay the entire log in order. This means that Copycat effectively must
persist the entire history of the log for failure recovery.

However, there are still ways to guard against an ever increasing log. Copycat uses a custom log compaction algorithm
in order to ensure that disk space is not completely consumed by commit logs.

Copycat's Raft implementation uses a key-based log to aid in reducing the size of the log. By assuming that only the last
instance of any given key needs to be retained (for instance, if a log key represents a may key state update), Copycat
can periodically evaluate the log for duplicate keys and remove old instances of any given key. This is accomplished by
writing the logs in segments. Periodically, a background thread will read a segment's keys into a memory efficient lookup
table. Once all the keys for a segment have been read in to memory, Copycat rewrites the segment with only the most recent
entries for each key, thus reducing the log size. This algorithm favors more recent segments over older segments under the
assumption that entries still active in older segments are less likely to be duplicated.

However, one issue arises in the case of tombstone entries. For instance, consider the scenario where a `remove` operation
stores an entry in Copycat's commit logs but effectively results in a lack of state. If the removed key is never written
to again, the entry will persist in the log for all of eternity even though it effectively results in no state. Copycat
guards against this scenario via its `Persistence` levels - specifically the `DURABLE` persistence level. Entries
persisted with the `DURABLE` persistence level are effectively treated as tombstones. Once a tombstone entry has been
replicated to *all active members of the cluster*, it is marked for deletion from the log. This ensures that all dependent
state machines have the opportunity to apply the tombstone, and eventually all tombstones will be removed from the log.

### Eventual consistency and Copycat's gossip protocol

While the [active members](#active-members) of the Copycat cluster perform consistent replication of
[resources](#resources) via the [Raft consensus protocol](#strong-consistency-and-copycats-raft-consensus-protocol),
[passive members](#passive-members) receive replicated logs via a simple gossip protocol. This allows Copycat to
support clusters much larger than a standard Raft cluster by making some consistency concessions.

#### Passive membership

Every member of the Copycat cluster participates in a simple gossip protocol that aids in cluster membership detection.
When a [passive member](#passive-members) joins the Copycat cluster, it immediately begins gossiping with the
configured [active members](#active-members) of the cluster. This gossip causes the active members to add the new
passive member to the cluster configuration, which they gossip with other members of the cluster, including passive
members. All gossip is performed by periodically selecting three random members of the cluster with which to share
membership information. The gossip protocol uses a vector clock to share membership information between nodes. When
a member gossips membership information to another member, it sends member and version information for each member
of the cluster. When a member receives gossiped membership information, it updates its local membership according to
the version of each member in the vector clock. This helps prevent false positives due to out-of-date cluster membership
information.

#### Log replication

Committed entries in resource logs are replicated to passive members of the cluster via the gossip protocol. Each member
of the cluster - both active and passive members - participates in gossip-based log replication, periodically selecting
three random members with which to gossip. A vector clock on each node is used to determine which entries to replicate
to each member in the random set of members. When a member gossips with another member, it increments its local version
and sends its vector clock containing last known indexes of each other member with the gossip. When a member receives
gossip from another member, it updates its local vector clock with the received vector clock, appends replicated entries
if the entries are consistent with its local log, increments its local version, and replies with its updated vector
clock of member indexes. Tracking indexes via vector clocks helps reduce the number of duplicate entries within the
gossip replication protocol.

### [User Manual](#user-manual)
