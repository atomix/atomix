Copycat
=======

## [User Manual](#user-manual)

Copycat is an extensible log-based distributed coordination framework for Java 8 built on the
[Raft consensus protocol](https://raftconsensus.github.io/).

### Overview

Copycat is a CP (consistent/partition-tolerant) oriented exercise in distributed coordination built on a consistent
replicated log. The core of Copycat is an extensible asynchronous framework that uses a mixture of
[gossip](http://en.wikipedia.org/wiki/Gossip_protocol) and the [Raft consensus protocol](https://raftconsensus.github.io/)
to provide a set of high level APIs that solve a variety of distributed systems problems including:
* [Leader election](#leader-elections)
* [Replicated state machines](#state-machines)
* [Strongly consistent state logs](#state-logs)
* [Eventually consistent event logs](#event-logs)
* [Distributed collections](#collections)
* [Resource partitioning](#resource-partitioning)
* [Failure detection](#failure-detection)
* [Remote execution](#remote-execution)

Copycat also provides integration with asynchronous networking frameworks like [Netty](http://netty.io) and
[Vert.x](http://vertx.io).

**Please note that Copycat is still undergoing heavy development, and until a beta release,
the API is subject to change.**

Copycat *will* be published to Maven Central once it is feature complete and well tested. Follow
the project for updates!

*Copycat requires Java 8. Though it has been requested, there are currently no imminent
plans for supporting Java 7.*

User Manual
===========

**Note: Some of this documentation may be inaccurate due to the rapid development currently
taking place on Copycat**

1. [Getting started](#getting-started)
   * [Adding Copycat as a Maven dependency](#adding-copycat-as-a-maven-dependency)
   * [Setting up the cluster](#setting-up-the-cluster)
   * [Configuring the protocol](#configuring-the-protocol)
   * [Creating a Copycat instance](#creating-a-copycat-instance)
   * [Creating a replicated state machine](#creating-a-replicated-state-machine)
   * [Querying the replicated state machine via proxy](#querying-the-replicated-state-machine-via-proxy)
1. [The Copycat dependency hierarchy](#the-copycat-dependency-hierarchy)
1. [State machines](#state-machines)
   * [Creating a state machine](#creating-a-state-machine)
   * [Configuring the state machine](#configuring-the-state-machine)
   * [Designing state machine states](#designing-state-machine-states)
   * [State machine commands](#state-machine-commands)
   * [State machine queries](#state-machine-queries)
   * [The state context](#the-state-context)
   * [Transitioning the state machine state](#transitioning-the-state-machine-state)
   * [Starting the state machine](#starting-the-state-machine)
   * [Working with state machine proxies](#working-with-state-machine-proxies)
   * [Partitioning a state machine](#partitioning-a-state-machine)
   * [Serialization](#serialization)
1. [Event logs](#event-logs)
   * [Creating an event log](#creating-an-event-log)
   * [Configuring the event log](#configuring-the-event-log)
   * [Writing events to the event log](#writing-events-to-the-event-log)
   * [Consuming events from the event log](#consuming-events-from-the-event-log)
   * [Replaying the log](#replaying-the-log)
1. [State logs](#state-logs)
   * [Creating a state log](#creating-a-state-log)
   * [Configuring the state log](#configuring-the-state-log)
   * [State commands](#state-commands)
   * [State queries](#state-queries)
   * [Submitting operations to the state log](#submitting-operations-to-the-state-log)
   * [Snapshotting](#snapshotting)
1. [Leader elections](#leader-elections)
   * [Creating a leader election](#creating-a-leader-election)
1. [Collections](#collections)
   * [AsyncMap](#asyncmap)
      * [Creating an AsyncMap](#creating-an-asyncmap)
      * [Configuring the AsyncMap](#configuring-the-asyncmap)
   * [AsyncList](#asynclist)
      * [Creating an AsyncList](#creating-an-asynclist)
      * [Configuring the AsyncList](#configuring-the-asynclist)
   * [AsyncSet](#asyncset)
      * [Creating an AsyncSet](#creating-an-asyncset)
      * [Configuring the AsyncSet](#configuring-the-asyncset)
   * [AsyncMultiMap](#asyncmultimap)
      * [Creating an AsyncMultiMap](#creating-an-asyncmultimap)
      * [Configuring the AsyncMultiMap](#configuring-the-asyncmultimap)
   * [AsyncLock](#asynclock)
      * [Creating an AsyncLock](#creating-an-asynclock)
      * [Configuring the AsyncLock](#configuring-the-asynclock)
1. [The Copycat cluster](#the-copycat-cluster)
   * [Cluster architecture](#cluster-architecture)
      * [Members](#members)
      * [Listeners](#listeners)
   * [Cluster configuration](#cluster-configuration)
   * [Leader election](#leader-election)
   * [Messaging](#messaging)
   * [Remote execution](#remote-execution)
1. [Protocols](#protocols)
   * [The local protocol](#the-local-protocol)
   * [Netty protocol](#netty-protocol)
   * [Vert.x protocol](#vertx-protocol)
   * [Vert.x 3 protocol](#vertx-3-protocol)
   * [Writing a custom protocol](#writing-a-custom-protocol)
1. [Architecture](#architecture)
   * [Strong consistency and Copycat's Raft consensus protocol](#strong-consistency-and-copycats-raft-consensus-protocol)
      * [Leader election](#leader-election-2)
      * [Command replication](#command-replication)
      * [Query consistency](#query-consistency)
      * [Log compaction](#log-compaction)
   * [Eventual consistency and Copycat's gossip protocol](#eventual-consistency-and-copycats-gossip-protocol)
      * [Log replication](#log-replication)
      * [Failure detection](#failure-detection)

## Getting started

### Adding Copycat as a Maven dependency
While Copycat provides many high level features for distributed coordination, the Copycat code base and architecture
is designed to allow each of the individual pieces of Copycat to be used independently of the others. However,
each Copycat component does have a common dependency on `copycat-core`.

Additionally, Copycat provides a single high-level `copycat-api` module which aggregates all the features provided
by Copycat. For first time users it is recommended that the `copycat-api` module be used.

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-api</artifactId>
  <version>0.5.0-SNAPSHOT</version>
</dependency>
```

### Configuring the protocol

Copycat supports a pluggable protocol that allows developers to integrate a variety of frameworks into the Copycat
cluster. Each Copycat cluster must specific a `Protocol` in the cluster's `ClusterConfig`. For more information
see the section on [protocols](#protocols).

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol());
```

Copycat core protocol implementations include [Netty](#netty-protocol), [Vert.x](#vertx), and [#Vert.x 3](vertx-3).
Each protocol implementation is separated into an independent module which can be added as a Maven dependency.

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-netty</artifactId>
  <version>0.5.0-SNAPSHOT</version>
</dependency>
```

### Setting up the cluster

In order to connect your `Copycat` instance to the Copycat cluster, you must add a set of protocol-specific URIs to
the `ClusterConfig`. The cluster configuration specifies how to find the *seed* nodes - the core voting members of the
Copycat cluster - by defining a simple list of seed node URIs. For more about seed nodes see the section on
[cluster members](#members).

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");
```

Note that member URIs must be unique and must agree with the configured protocol instance. Member URIs will be checked
at configuration time for validity.

Because of the design of the [Raft algorithm](#strong-consistency-and-copycats-raft-consensus-protocol), it is strongly
recommended that any Copycat cluster have *at least three voting members*.

### Creating a Copycat instance

To create a `Copycat` instance, call one of the overloaded `Copycat.create()` methods:
* `Copycat.create(String uri, ClusterConfig cluster)`
* `Copycat.create(String uri, ClusterConfig cluster, Executor executor)`
* `Copycat.create(String uri, ClusterConfig cluster, CopycatConfig config)`
* `Copycat.create(String uri, ClusterConfig cluster, CopycatConfig config, Executor executor)`

Note that the first argument to any `Copycat.create()` method is a `uri`. This is the protocol specific URI of the
*local* member, and it may or may not be a member defined in the provided `ClusterConfig`. This is because Copycat
actually supports eventually consistent replication for clusters much larger than the core Raft cluster - the cluster
of *seed* members defined in the cluster configuration.

```java
Copycat copycat = Copycat.create("tcp://123.456.789.3", cluster);
```

When a `Copycat` instance is constructed, a central replicated state machine is created for the entire Copycat cluster.
This state machine is responsible for maintaining the state of all cluster resources. In other words, it acts as a
central registry for other log based structures created within the cluster. This allows Copycat to coordinate the
creation, usage, and deletion of multiple log-based resources within the same cluster.

### Creating a replicated state machine
The most common use case for consensus algorithms is to support a strongly consistent replicated state machine.
Replicated state machines are similarly an essential feature of Copycat. Copycat provides a unique, proxy based
interface that allows users to operate on state machines either synchronously or asynchronously.

To create a replicated state machine, you first must design an interface for the state machine states. In this case
we'll use one of Copycat's provided [collections](#collections) as an example.

States are used internally by Copycat to apply state machine commands and queries and transition between multiple
logical states. Each state within a state machine must implement a consistent explicitly defined interface. In this
case we're using the existing Java `Map` interface to define the state.

```java
public class DefaultAsyncMapState<K, V> implements Map<K, V> {
  private Map<K, V> map;

  @Initializer
  public void init(StateContext<AsyncMapState<K, V>> context) {
    map = context.get("value");
    if (map == null) {
      map = new HashMap<>();
      context.put("value", map);
    }
  }

  @Override
  public V put(K key, V value) {
    return map.put(key, value);
  }

  @Override
  public V get(K key) {
    return map.get(key);
  }

  @Override
  public V remove(K key) {
    return map.remove(key);
  }

  @Override
  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

}
```

The `StateContext` can be used to store state values that Copycat will use to take snapshots of the state machine state
and compact the replicated log. For this reason, it is essential that state implementations store state in the
`StateContext`.

Once we've defined the map state, we can simply create a new map state machine via the `stateMachine` factory method.

```java
StateMachine<Map<String, String>> stateMachine = copycat.stateMachine("map", Map.class, new DefaultMapState<>()).get();
stateMachine.open().get();
```

When a Copycat resource - such as a state machine, log, or election - is created, the resource must be opened before
it can be used. Practically all Copycat interfaces return `CompletableFuture` instances which can either be used to
block until the result is received or receive the result asynchronously. In this case we just block by calling the
`get()` method on the `CompletableFuture`.

### Querying the replicated state machine via proxy

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

## The Copycat dependency hierarchy
The Copycat project is organized into a number of modules based on specific use cases.

#### copycat-api
The `copycat-api` module is a high-level project that aggregates all the separate Copycat modules.

#### copycat-core
The `copycat-core` module is the core of Copycat which provides the base Raft implementation, configuration,
cluster management, messaging, logs, and protocol interfaces.

#### event-log
The `copycat-event-log` module is an event log implementation built on the Raft consensus protocol.
The event log supports both small strongly consistent event logs and large eventually consistent event
logs via a gossip protocol.

#### state-log
The `copycat-state-log` module is a strongly consistent, snapshottable log built on the Raft consensus protocol.

#### state-machine
The `copycat-state-machine` module provides a high-level state machine API on top of the `copycat-state-log` API.

#### leader-election
The `copycat-leader-election` module provides a simple Raft-based distributed leader election API.

#### collections
The `copycat-collections` module provides strongly- and eventually-consistent log-based distributed data
structures including `AsyncMap`, `AsyncMultiMap`, `AsyncList`, `AsyncSet`, and `AsyncLock`.

#### copycat-chronicle
The `copycat-chronicle` module is a fast [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) based log
implementation.

#### netty
The `copycat-netty` module provides a [Netty](http://netty.io) based protocol implementation.

#### vertx
The `copycat-vertx` module provides a [Vert.x 2](http://vertx.io) based protocol implementation.

#### vertx3
The `copycat-vertx3` module provides a [Vert.x 3](http://vertx.io) based protocol implementation.

## State machines

### Creating a state machine

### Configuring the state machine

### Designing state machine states

### State machine commands

### State machine queries

### The state context

### Transitioning the state machine state

### Starting the state machine

### Working with state machine proxies

### Partitioning a state machine

### Serialization

## Event logs

### Creating an event log

### Configuring the event log

### Writing events to the event log

### Consuming events from the event log

### Replaying the log

## State logs

### Creating a state log

### Configuring the state log

### State commands

### State queries

### Submitting operations to the state log

### Snapshotting

## Leader elections

### Creating a leader election

## Collections

### AsyncMap

#### Creating an AsyncMap

#### Configuring the AsyncMap

### AsyncList

#### Creating an AsyncList

#### Configuring the AsyncList

### AsyncSet

#### Creating an AsyncSet

#### Configuring the AsyncSet

### AsyncMultiMap

#### Creating an AsyncMultiMap

#### Configuring the AsyncMultiMap

### AsyncLock

#### Creating an AsyncLock

#### Configuring the AsyncLock

## The Copycat cluster

### Cluster architecture

#### Members

#### Listeners

### Cluster configuration

### Leader election

### Messaging

### Remote execution

## Protocols

### The local protocol

### Netty protocol

### Vert.x protocol

### Vert.x 3 protocol

### Writing a custom protocol

## Architecture

### Strong consistency and Copycat's Raft consensus protocol

#### Leader election

#### Command replication

#### Query consistency

#### Log compaction

### Eventual consistency and Copycat's gossip protocol

#### Log replication

#### Failure detection

### [User Manual](#user-manual)
