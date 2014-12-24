Copycat
=======

## [User Manual](#user-manual)

Copycat is an extensible log-based distributed coordination framework for Java 8 built on the
[Raft consensus protocol](https://raftconsensus.github.io/).

The core of Copycat is a set of high level APIs for consistent distributed coordination based on a replicated log.
Copycat provides concise asynchronous APIs for replicated, consistent, partition-tolerant event logs, state logs,
state machines, leader elections, and collections.

Copycat also provides integration with asynchronous networking frameworks like [Netty](http://netty.io) and
[Vert.x](http://vertx.io).

**Please note that Copycat is still undergoing heavy development, and until a beta release,
the API is subject to change.**

Copycat *will* be published to Maven Central it is feature complete and well tested. Follow
the project for updates!

*Copycat requires Java 8. Though it has been requested, there are currently no imminent
plans for supporting Java 7.*

User Manual
===========

**Note: Some of this documentation may be inaccurate due to the rapid development currently
taking place on Copycat**

1. [Getting started](#getting-started)
   * [Configuring the cluster](#configuring-the-cluster)
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
1. [Protocols]
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

### Configuring the cluster

In order to connect to the Copycat cluster, you first must create a `ClusterConfig` defining the protocol
to use for communication and a set of seed nodes in the cluster. Seed nodes are the permanent voting members of
the cluster to which passive members connect and gossip with.

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");
```

### Creating a Copycat instance

### Creating a replicated state machine

### Querying the replicated state machine via proxy

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
