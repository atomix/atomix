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
1. [The Copycat API](#the-copycat-api)
   * [Creating a Copycat instance](#creating-a-copycat-instance)
   * [Accessing the Copycat cluster](#accessing-the-copycat-cluster)
1. [Resources](#resources)
   * [Resource lifecycle](#resource-lifecycle)
   * [Configuring resources](#configuring-resources)
   * [Creating resources](#creating-resources)
   * [Resource clusters](#resource-clusters)
1. [State machines](#state-machines)
   * [Creating a state machine](#creating-a-state-machine)
   * [Creating a standalone state machine](#creating-a-state-machine-as-a-standalone-service)
   * [Configuring the state machine](#configuring-the-state-machine)
   * [Designing state machine states](#designing-state-machine-states)
   * [State machine commands](#state-machine-commands)
   * [State machine queries](#state-machine-queries)
   * [The state context](#the-state-context)
   * [Transitioning the state machine state](#transitioning-the-state-machine-state)
   * [Synchronous proxies](#synchronous-proxies)
   * [Asynchronous proxies](#asynchronous-proxies)
1. [Event logs](#event-logs)
   * [Creating an event log](#creating-an-event-log)
   * [Configuring the event log](#configuring-the-event-log)
   * [Writing events to the event log](#writing-events-to-the-event-log)
   * [Consuming events from the event log](#consuming-events-from-the-event-log)
   * [Working with event log partitions](#working-with-event-log-partitions)
   * [Event log clusters](#event-log-clusters)
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
   * [AsyncList](#asynclist)
   * [AsyncSet](#asyncset)
   * [AsyncMultiMap](#asyncmultimap)
   * [AsyncLock](#asynclock)
1. [The Copycat cluster](#the-copycat-cluster)
   * [Members](#members)
      * [Active members](#active-members)
      * [Passive members](#passive-members)
      * [Member states](#member-states)
   * [Leader election](#leader-election)
   * [Direct messaging](#direct-messaging)
   * [Broadcast messaging](#broadcast-messaging)
   * [Remote execution](#remote-execution)
1. [Protocols](#protocols)
   * [The local protocol](#the-local-protocol)
   * [Netty protocol](#netty-protocol)
   * [Vert.x protocol](#vertx-protocol)
   * [Vert.x 3 protocol](#vertx-3-protocol)
1. [Architecture](#architecture)
   * [Strong consistency and Copycat's Raft consensus protocol](#strong-consistency-and-copycats-raft-consensus-protocol)
      * [Leader election](#leader-election-2)
      * [Command replication](#command-replication)
      * [Query consistency](#query-consistency)
      * [Log compaction](#log-compaction)
   * [Eventual consistency and Copycat's gossip protocol](#eventual-consistency-and-copycats-gossip-protocol)
      * [Log replication](#log-replication)
      * [Failure detection](#failure-detection)
1. [The Copycat dependency hierarchy](#the-copycat-dependency-hierarchy)

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

The `copycat-core` module also contains a `LocalProtocol` which can be used for multi-threaded testing. When using
the `LocalProtocol` for testing, you should use the same `LocalProtocol` instance across all test instances.

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

## The Copycat API
Copycat provides a high-level `Copycat` API that aggregates each of the Copycat [resource types](#resources) into a
single fluent API.

### Creating a Copycat instance

To create a `Copycat` instance, call one of the overloaded `Copycat.create()` methods:
* `Copycat.create(String uri, ClusterConfig cluster)`
* `Copycat.create(String uri, CopycatConfig config)`

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

Once the `Copycat` instance has been created, you must open the `Copycat` instance by calling the `open` method.

```java
CompletableFuture<Copycat> future = copycat.open();
future.get();
```

When the `Copycat` instance is opened, all the [resources](#resources) defined in the
[configuration](#configuring-resources) will be opened and begin participating in leader election and log replication.

### Accessing the Copycat cluster
Each `Copycat` instance contains a reference to the global `Cluster`. The cluster handles communication between
Copycat instances and provides a vehicle through which users can [communicate](#messaging) with other Copycat instances
as well. To get a copy of the Copycat `Cluster` simply call the `cluster` getter on the `Copycat` instance.

```java
Cluster cluster = copycat.cluster();
```

For more information on using the `Cluster` object to communicate with other members of the cluster see the section
on [the Copycat cluster](#the-copycat-cluster).

## Resources

Each Copycat instance can support any number of various named resources. *Resource* is an abstract term for all of the
high level log-based data types provided by Copycat. Ultimately, each resource - whether is be an event log, state log,
state machine, or collection - is backed by a Raft replicated log that is managed by the Copycat cluster coordinator
internally.

### Resource lifecycle

When a Copycat instance is created and opened, any resources for which the local `Copycat` instance is listed as a
replica will be opened internally and immediately begin participating in leader election and replication for that
resource. Leader election and replication for resources occur separately from the global Copycat cluster. Therefore,
the leader for a given resource may frequently differ from the global Copycat cluster leader. This ensures that Copycat
can maintain consistency across multiple resources within the same cluster.

### Configuring resources

It's important to note that *all cluster resources must be configured prior to opening the Copycat instance.* This is
because Copycat needs a full view of the cluster's resources in order to properly configure and open various resources
internally. For instance, if a state log is created on node *a* in a three node cluster, nodes *b* and *c* also need
to create the same log in order to support replication.

Each of Copycat's resource types - `EventLog`, `StateLog`, `StateMachine`, etc - has an associated `Config` class.
This configuration class is used to configure various attributes of the resource, including Raft-specific configuration
options such as election timeouts and heartbeat intervals as well as resource specific configuration options. Each of
these configuration classes can then be added to the global `CopycatConfig` prior to startup.

```java
CopycatConfig config = new CopycatConfig()
  .withClusterConfig(new ClusterConfig()
    .withProtocol(new NettyTcpProtocol())
    .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2"));

config.addEventLogConfig("event-log", new EventLogConfig()
  .withSerializer(KryoSerializer.class)
  .withLog(new FileLog()
    .withSegmentSize(1024 * 1024)
    .withRetentionPolicy(new SizeBasedRetentionPolicy(1024 * 1024)));
```

The first argument to any `addResourceConfig` type method is always the unique resource name. Resource names are unique
across the entire cluster, not just for the specific resource type. So, if you attempt to overwrite an existing
resource configuration with a resource configuration of a different type, a `ConfigurationException` will occur.

Each resource can optionally define a set of replicas separate from the global cluster configuration. The set of
replicas in the resource configuration *must be contained within the set of seed nodes defined in the cluster
configuration*. The resource's replica set defines the members that should participate in leader election and
synchronous replication for the resource. Therefore, in a five node Copycat cluster, if a resource is configured with
only three replicas, writes will only need to be persisted on two nodes in order to be successful.

```java
CopycatConfig config = new CopycatConfig()
  .withClusterConfig(new ClusterConfig()
    .withProtocol(new NettyTcpProtocol())
    .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2", "tcp://123.456.789.3", "tcp://123.456.789.4"));

config.addEventLogConfig("event-log", new EventLogConfig()
  .withSerializer(KryoSerializer.class)
  .withReplicas("tcp://123.456.789.1", "tcp://123.456.789.2", "tcp://123.456.789.3")
  .withLog(new FileLog()
    .withSegmentSize(1024 * 1024)
    .withRetentionPolicy(new SizeBasedRetentionPolicy(1024 * 1024)));
```

The Copycat configuration API is designed to support arbitrary `Map` based configurations as well. Simply pass a map
with the proper configuration options for the given configuration type to the configuration object constructor:

```java
// Create an event log configuration map.
Map<String, Object> configMap = new HashMap<>();
configMap.put("serializer", "net.kuujo.copycat.util.serializer.KryoSerializer");

// Create a file-based log configuration map.
Map<String, Object> logConfigMap = new HashMap<>();
logConfigMap.put("class", "net.kuujo.copycat.log.FileLog");
logConfigMap.put("segment.size", 1024 * 1024);

// Create a log retention policy configuration map.
Map<String, Object> retentionConfigMap = new HashMap<>();
retentionConfigMap.put("class", "net.kuujo.copycat.log.SizeBasedRetentionPolicy");
retentionConfigMap.put("size", 1024 * 1024);

// Add the log retention policy to the log configuration map.
logConfigMap.put("retention-policy", retentionConfigMap);

// Add the log configuration map to the event log configuration.
configMap.put("log", logConfigMap);

// Construct the event log.
EventLogConfig config = new EventLogConfig(configMap);
```

### Creating resources

With resources configured and the `Copycat` instance created, resources can be easily retrieved by calling any
of the resource-specific methods on the `Copycat` instance:
* `<T, U> EventLog<T, U> eventLog(String name)`
* `<T, U> StateLog<T, U> stateLog(String name)`
* `<T> StateMachine<T> stateMachine(String name)`
* `LeaderElection leaderElection(String name)`
* `<K, V> AsyncMap<K, V> map(String name)`
* `<K, V> AsyncMultiMap<K, V> multiMap(String name)`
* `<T> AsyncList<T> list(String name)`
* `<T> AsyncSet<T> set(String name)`
* `AsyncLock lock(String name)`

```java
Copycat copycat = Copycat.create("tcp://123.456.789.0", config);

copycat.open().thenRun(() -> {
  StateMachine<String> stateMachine = copycat.stateMachine("test");
  stateMachine.open().thenRun(() -> {
    Map<String, String> map = stateMachine.createProxy(Map.class);
    map.put("foo", "Hello world!").thenRun(() -> {
      map.get("foo").thenAccept(value -> {
        System.out.println(value);
      });
    });
  });
});
```

Java 8's `CompletableFuture` framework is extremely powerful. For instance, the previous code sample could be rewritten
as so:

```java
Copycat copycat = Copycat.create("tcp://123.456.789.0", config);

copycat.open()
  .thenCompose(copycat.stateMachine("test").open())
  .thenApply(stateMachine -> stateMachine.createProxy(Map.class))
  .thenAccept(map -> {
    map.put("foo", "Hello world!")
      .thenCompose(v -> map.get("foo"))
      .thenAccept(value -> System.out.println(value));
  });
```

### Resource clusters

In order to ensure consistency across resources, each resource performs leader election and replication separately
from the global `Copycat` cluster. Additionally, because each resource can be replicated on a separate set of nodes
than that which is defined in the global `ClusterConfig`, resources maintain a separate membership list as well.
Each resource exposes a separate `Cluster` instance that contains state regarding the resource's leader election and
membership. This `Cluster` can be used to communicate with other members in the resource's cluster.

```java
Cluster cluster = stateMachine.cluster();
```

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
public class DefaultMapState<K, V> implements Map<K, V> {
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
CopycatConfig config = new CopycatConfig()
  .withClusterConfig(cluster)
  .addStateMachineConfig("map", new StateMachineConfig()
    .withStateType(Map.class)
    .withInitialState(DefaultMapState.class));

Copycat copycat = Copycat.create("tcp://123.456.789.0", config).open().get();

StateMachine<Map<String, String>> stateMachine = copycat.stateMachine("map").open().get();
```

When a Copycat resource - such as a state machine, log, or election - is created, the resource must be opened before
it can be used. Practically all Copycat interfaces return `CompletableFuture` instances which can either be used to
block until the result is received or receive the result asynchronously. In this case we just block by calling the
`get()` method on the `CompletableFuture`.

### Creating a state machine as a standalone service

Copycat's architecture is designed such that individual resource types can be used completely independently of others.
The state machine module can be added as a separate Maven dependency, and state machines can be created independent
of the high level `Copycat` interface which aggregates all resource types into a single component.

To use the state machine as a standalone service, simply add the `copycat-state-machine` module as a Maven dependency:

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-state-machine</artifactId>
  <version>0.5.0-SNAPSHOT</version>
</dependency>
```

The `StateMachine` interface provides its own constructors similar to the `Copycat.create` constructors.

```java
StateMachineConfig config = new StateMachineConfig()
  .withStateType(Map.class)
  .withInitialState(DefaultMapState.class)'

StateMachine<Map<K, V>> stateMachine = StateMachine.create("tcp://123.456.789.0", cluster, config).open().get();
stateMachine.submit("get", "foo").get();
```

### Configuring the state machine

State machines are configured using the `StateMachineConfig` class. This class contains vital configuration information
regarding how the state machine should handle startup and how it should apply commands.

As demonstrated in the example in the previous section, two vital components of the `StateMachineConfig` are the
*state type* and *initial state*. The state type is an *interface* which contains all of the state methods. All state
implementations must implement this interface. The initial state is the first state to which the state machine will
transition upon startup.

```java
StateMachineConfig config = new StateMachineConfig()
  .withStateType(Map.class)
  .withInitialState(DefaultMapState.class);
```

By default, all Copycat resources use the `KryoSerializer` for serialization. This should work fine for most use cases,
but if you so desire you can configure the `Serializer` class on the `StateMachineConfig`.

```java
StateMachineConfig config = new StateMachineConfig()
  .withStateType(Map.class)
  .withInitialState(DefaultMapState.class)
  .withSerializer(MyCustomSerializer.class);
```

### Designing state machine states

### State machine commands

State machine commands are annotated methods on the state machine state interface. Annotating a method with the
`@Command` annotation indicates that it is a stateful method call, meaning the operation that calls the method should
be persisted to the log and replicated. It is vital that *all* methods which alter the state machine's state be
identified by the `@Command` annotation. For this reason, all state machine methods are commands by default.

```java
public interface AsyncMap<K, V> {

  @Command
  V put(K key);

}
```

### State machine queries

Queries are the counter to commands. The `@Query` annotation is used to identify state machine methods which are
purely read-only methods. *You should never annotate a method that alters the state machine state with the query
annotation.* If methods which alter the state machine state are not annotated as commands, calls to the method will
*not* be logged and replicated, and thus state will be inconsistent across replicas.

```java
public interface AsyncMap<K, V> {

  @Query
  V get(K key);

}
```

You can specify the required consistency of queries by defining the `@Query` annotation's `consistency` argument.
The query consistency allows you to control how read operations are performed. Copycat supports three consistency
levels:
* `WEAK` - reads the state machine from the local node. This is the cheapest/fastest consistency level, but will often
  result in stale data being read
* `DEFAULT` - all reads go through the resource leader which performs consistency checks based on a lease period equal to the
  resource's heartbeat interval
* `STRONG` - all reads go through the resource leader which performs a synchronous consistency check with a majority of
  the resource's replicas before applying the query to the state machine and returning the result. This is the most
  expensive consistency level.

Query consistency defaults to `DEFAULT`

```java
public interface MapState<K, V> extends Map<K, V> {

  @Override
  @Query(consistency=Consistency.STRONG)
  V get(K key);

}
```

### The state context

Copycat's state machine provides a `StateContext` object in which state implementations should store state. The reason
for storing state in a separate object rather than in the states themselves is threefold. First, the `StateContext`
object persists throughout the lifetime of the state machine, even across states. Second, the state machine uses the
state held within the `StateContext` to take and install snapshots for log compaction. Snapshotting and log compaction
is performed automatically using the context's state; users need only store state in the context in order for snapshots
to work. Third, the state context provides a vehicle through which states can transition to other states.

To get the `StateContext` object for the state machine, simply add a method to your state annotated with the
`@Initializer` annotation.

```java
public class DefaultMap<K, V> implements Map<K, V> {
  private StateContext<AsyncMap<K, V>> context;

  @Initializer
  public void init(StateContext<AsyncMap<K, V>> context) {
    this.context = context;
  }

  public V get(K key) {
    return context.<Map<K, V>>get("the-map").get(key);
  }

}
```

### Transitioning the state machine state

To transition to another state via the `StateContext`, call the `transition` method. A perfect example of this is
a lock. This example transitions between two states - locked and unlocked - based on the current state.

```java
public class UnlockedLockState implements LockState {
  private StateContext<AsyncMap<K, V>> context;

  @Initializer
  public void init(StateContext<AsyncMap<K, V>> context) {
    this.context = context;
  }

  public void lock() {
    context.transition(new LockedLockState());
  }

  public void unlock() {
    throw new IllegalStateException("Lock is not locked");
  }

}
```

```java
public class LockedLockState implements LockState {
  private StateContext<AsyncMap<K, V>> context;

  @Initializer
  public void init(StateContext<AsyncMap<K, V>> context) {
    this.context = context;
  }

  public void lock() {
    throw new IllegalStateException("Lock is locked");
  }

  public void unlock() {
    context.transition(new UnlockedLockState());
  }

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
Event logs are inherently partitioned - allowing high-throughput concurrent writes to leaders of multiple partitions -
and entries are consumed as they are received.

### Creating an event log

To create an event log, call the `eventLog` method on the `Copycat` instance.

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

CopycatConfig config = new CopycatConfig()
  .withClusterConfig(cluster)
  .addEventLogConfig("event-log", new EventLogConfig())
    .withPartitions(3);

Copycat copycat = Copycat.create("tcp://123.456.789.0", config);

copycat.open().thenRun(() -> {
  copycat.<String, String>eventLog("event-log").open().thenAccept(eventLog -> {
    eventLog.commit("Hello world!");
  });
});
```

Alternatively, an event log can be created independent of the high-level `Copycat` API by simply adding the
`copycat-event-log` module as a dependency directly and instantiating a new event log via the `EventLog.create`
static interface method:

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

EventLogConfig config = new EventLogConfig())
  .withPartitions(3);

EventLog<String, String> eventLog = EventLog.create("tcp:/123.456.789.0", cluster, config);
```

Note that the event log API requires two generic types. The first generic - `T` - is the partition key type. This is
the data type of the keys used to select partitions to which to commit entries. The second generic type - `U` - is the
log entry type.

### Configuring the event log

Copycat event logs are partitioned and replicated. Thus, the `EventLogConfig` API provides various methods for
configuring the partitioning and replication of each event log.

To set the number of event log partitions, use the `setPartitions` method or `withPartitions` fluent method.

```java
EventLogConfig config = new EventLogConfig()
  .withPartitions(3);
```

The number of partitions indicated will dictate the number of separate replicated log instances that are created for
the event log. For each log partition, a separate leader will be elected and separate instance of the Raft consensus
algorithm will be run.

In order to control the amount of cluster resources each event log partition consumes, Copycat provides a *replication
factor* option which configures the number of nodes on which each partition is synchronously replicated. For each
level of replication factor, two additional nodes are required for replication for each partition. In other words:
* A replication factor of `0` indicates that each partition resides only on a single node
* A replication factor of `1` indicates that each partition resides on three nodes, and writes require data to be
  written to the leader and one other node
* A replication factor of `2` indicates that each partition resides on five nodes, and writes require data to be written
  to the leader and two other nodes

To configure the replication factor, use the `setReplicationFactor` method or `withReplicationFactor` fluent method.

```java
EventLogConfig config = new EventLogConfig()
  .withPartitions(3)
  .withReplicationFactor(1);
```

By default the replication factor is `-1`, indicating that each partition should be replicated to the entire cluster.

### Writing events to the event log

The `EventLog` API provides several methods for writing events to the log. The simplest method is by simply using
the `commit(U entry)` method.

```java
eventLog.commit("Hello world!");
```

Because the event log is partitioned, each entry committed to the event log must be
routed to a specific partition. In the case of the `commit(U entry)` method, a partition is selected by simply using
the mod hash of the given entry to select a partition.

If you want to get more control over how logs are partitioned, the second method for committing entries to the event
log is via the `commit(T partitionKey, U entry)` method.

```java
eventLog.commit("fruit", "apple");
eventLog.commit("vegetable", "carrot");
```

This method accepts a partition key which is used to perform the same simple mod hash algorithm to select a partition
to which to commit the entry.

Finally, `EventLog` is a `PartitionedResource` which exposes methods for directly accessing log partitions. This allows
users to arbitrarily partition event log entries in whatever way they want.

To get a list of event log partitions, use the `partitions` method:

```java
for (EventLogPartition partition : eventLog.partitions()) {
  partition.commit(entry);
}
```

To get a specific event log partition, use the `partition` method, passing the partition number to get. Note that
partition numbers start at `1`, not `0`.

```java
eventLog.partition(1).commit("Hello world!");
```

### Consuming events from the event log

To consume messages from the event log, register a message consumer via the `consumer` method:

```java
eventLog.consumer(entry -> System.out.println(entry));
```

### Event log clusters

Because the `EventLog` is a partitioned resource, and because each partition performs leader election and replication
separate from its sibling partitions, the `EventLog` API itself does not expose a `Cluster` as with other resources.
Instead, event log clusters must be accessed on a per-partition basis.

```java
eventLog.partition(1).cluster().election().addListener(result -> System.out.println("Winner: " + result.winner()));
```

## State logs
State logs are strongly consistent Raft replicated logs designed for persisting state. These logs are the basis of
Copycat's state machine and the data structures built on top of it. Whereas event logs are designed for time/size based
compaction, state logs support log compaction via snapshotting, allowing state to be persisted to the log and
replicated to other nodes in the cluster whenever necessary.

### Creating a state log

To create a state log, call the `stateLog` method on the `Copycat` instance.

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

CopycatConfig config = new CopycatConfig()
  .withClusterConfig(cluster)
  .addStateLogConfig("state-log", new StateLogConfig())
    .withPartitions(3);

Copycat copycat = Copycat.create("tcp://123.456.789.0", config);

copycat.open().thenRun(() -> {
  copycat.<String, String>stateLog("state-log").open().thenAccept(stateLog -> {
    stateLog.commit("Hello world!");
  });
});
```

Alternatively, a state log can be created independent of the high-level `Copycat` API by simply adding the
`copycat-state-log` module as a dependency directly and instantiating a new state log via the `StateLog.create`
static interface method:

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

StateLogConfig config = new StateLogConfig())
  .withPartitions(3);

StateLog<String, String> stateLog = StateLog.create("tcp:/123.456.789.0", cluster, config);
```

Note that the state log API requires two generic types. The first generic - `T` - is the partition key type. This is
the data type of the keys used to select partitions to which to commit entries. The second generic type - `U` - is the
log entry type.

### Configuring the state log

Copycat state logs are partitioned and replicated. Thus, the `StateLogConfig` API provides various methods for
configuring the partitioning and replication of each state log.

To set the number of state log partitions, use the `setPartitions` method or `withPartitions` fluent method.

```java
StateLogConfig config = new StateLogConfig()
  .withPartitions(3);
```

The number of partitions indicated will dictate the number of separate replicated log instances that are created for
the state log. For each log partition, a separate leader will be elected and separate instance of the Raft consensus
algorithm will be run.

In order to control the amount of cluster resources each state log partition consumes, Copycat provides a *replication
factor* option which configures the number of nodes on which each partition is synchronously replicated. For each
level of replication factor, two additional nodes are required for replication for each partition. In other words:
* A replication factor of `0` indicates that each partition resides only on a single node
* A replication factor of `1` indicates that each partition resides on three nodes, and writes require data to be
  written to the leader and one other node
* A replication factor of `2` indicates that each partition resides on five nodes, and writes require data to be written
  to the leader and two other nodes

To configure the replication factor, use the `setReplicationFactor` method or `withReplicationFactor` fluent method.

```java
StateLogConfig config = new StateLogConfig()
  .withPartitions(3)
  .withReplicationFactor(1);
```

By default the replication factor is `-1`, indicating that each partition should be replicated to the entire cluster.

### State commands

State logs work to alter state by applying log entries to registered state commands. Commands are operations which
alter the state machine state. When submitted to the state log, all commands (writes) go through the resource leader
and are logged and replicated prior to being applied to the command function.

To register a state command, use the `registerCommand` method.

```java
stateLog.registerCommand("add", (partitionKey, entry) -> data.add(entry));
```

Note also that commands can be registered on each partition of the state log individually.

```java
stateLog.partition(1).registerCommand("put", entry -> data.add(entry));
```

### State queries

As you can see above state logs separate *read* operations from *write* operations, and for good reason. Write
operations require strong consistency and persistence, while read operations - which don't alter the state of the log -
can be performed without persistence. To register a read operation on the state log, use the `registerQuery` method:

```java
stateLog.registerQuery("get", (partitionKey, index) -> data.get(index));
```

Because of the flexibility of read-only operations, Copycat provides several consistency modes with which users can
trade consistency for performance and vice versa.
* `STRONG` - Guarantees strong consistency of all reads by performing reads through the leader which contacts a majority
  of nodes to verify the cluster state prior to responding to the request
* `DEFAULT` - Promotes consistency using a leader lease with which the leader assumes consistency for a period of time
  before contacting a majority of the cluster in order to check consistency
* `WEAK` - Allows stale reads from the local node

To specify the query consistency, simply pass an additional `Consistency` parameter to `registerQuery`:

```java
stateLog.registerQuery("get", (partitionKey, index) -> data.get(index), Consistency.FULL);
```

Note also that queries can be registered on each partition of the state log individually.

```java
stateLog.partition(1).registerQuery("get", index -> data.get(index));
```

### Submitting operations to the state log

Once [commands](#state-commands) and [queries](#state-queries) have been registered on the log, commands and queries
can be submitted to the log. To submit a named operation to the log, call the `submit` method.

```java
stateLog.submit("add", "Hello world!");
```

As with the event log, state logs perform partitioning internally using a simple mod hash algorithm. To provide a custom
partition key by which to partition the log, pass the partition key as the second argument to `submit`.

```java
stateLog.submit("add", "fruit", "apple");
```

Alternatively, you can access the state log partitions directly:

```java
stateLog.partition(1).submit("add", "Hello world!");
```

### Snapshotting

Up until this point, if the state log were allowed to run unencumbered for a long enough period of time, presumably
your machines would run out of disk space for the log. Fortunately, the Raft algorithm has a mechanism for compacting
the state log: snapshots. By registering a snapshot handler on the state log, Copycat will periodically call the handler
and replace a portion of the log with the state returned by the snapshot provider. Then, in the event of a failure the
snapshot installer will be called to repopulate the state from the last snapshot.

To register a snapshot provider on the state log, use the `snapshotWith` methods:

```java
stateLog.snapshotWith(partition -> data);
```

```java
stateLog.partition(1).snapshotWith(() -> data);
```

Any snapshot provider should be accompanied by a snapshot installer as well.

```java
stateLog.installWith((partition, data) -> {
  this.data = data;
});
```

```java
stateLog.partition(1).installWith(data -> {
  this.data = data;
});
```

Don't worry, Copycat will handle all the complexity of persisting, loading, and replicating snapshots. If you want to
see what can be built on top of the Copycat state log, see Copycat's replicated [state machine](#state-machine)
implementation.

## Leader elections

Leader elections are perhaps the simplest feature of Copycat, but often one of the most useful as well. One of the more
significant challenges faced by many distributed systems is coordinating leadership among a set of nodes. Copycat's
simple Raft based leader election implementation allows users to elect a leader within a resource cluster and then
use Copycat's messaging and remote execution features to react on that leader election.

Note that the leader election resource type simply wraps the resource `Cluster` based `LeaderElection` API. In reality,
all Copycat resources participate in elections, and resource elections can be accessed via each respective resource's
`Cluster`.

### Creating a leader election

To create a `LeaderElection` via the `Copycat` API, add the `LeaderElectionConfig` to your `CopycatConfig`.

```java
CopycatConfig config = new CopycatConfig()
  .withClusterConfig(cluster)
  .addElectionConfig("election", new LeaderElectionConfig());
```

Once the leader election has been defined as a cluster resource, create a new `Copycat` instance and get the leader
election. To register a handler to be notified once a node has become leader, use the `addListener` method.

```java
Copycat copycat = Copycat.create("tcp://123.456.789.0", config);

copycat.open()
  .thenCompose(c -> c.leaderElection("election").open())
  .thenAccept(election -> {
    election.addListener(member -> System.out.println(member.uri() + " was elected leader!");
  });
```

## Collections

In a partially academic effort, Copycat provides a variety of strongly consistent data structures built on top of its
[state machine](#state-machines) framework.

### AsyncMap

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

CopycatConfig config = new CopycatConfig()
  .withClusterConfig(cluster)
  .addMapConfig("test-map", new AsyncMapConfig()
    .withConsistency(Consistency.STRONG));

Copycat.copycat("tcp://123.456.789.0", config).open()
  .thenApply(copycat -> copycat.<String, String>map("test-map"))
  .thenCompose(map -> map.open())
  .thenAccept(map -> {
    map.put("foo", "Hello world!").thenRun(() -> {
      map.get("foo").thenAccept(result -> System.out.println(result));
    });
  });
```

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

AsyncMapConfig config = new AsyncMapConfig()
  .withConsistency(Consistency.STRONG);

AsyncMap.<String, String>create("tcp://123.456.789.0", cluster, config).open().thenAccept(map -> {
  map.put("foo", "Hello world!").thenRun(() -> {
    map.get("foo").thenAccept(result -> System.out.println(result));
  });
});
```

### AsyncList

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

CopycatConfig config = new CopycatConfig()
  .withClusterConfig(cluster)
  .addListConfig("test-list", new AsyncListConfig()
    .withConsistency(Consistency.STRONG));

Copycat.copycat("tcp://123.456.789.0", config).open()
  .thenApply(copycat -> copycat.<String>list("test-list"))
  .thenCompose(list -> list.open())
  .thenAccept(list -> {
    list.add("Hello world!").thenRun(() -> {
      list.get(0).thenAccept(result -> System.out.println(result));
    });
  });
```

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

AsyncListConfig config = new AsyncListConfig()
  .withConsistency(Consistency.STRONG);

AsyncList.<String>create("tcp://123.456.789.0", cluster, config).open().thenAccept(list -> {
  list.add("Hello world!").thenRun(() -> {
    list.get(0).thenAccept(result -> System.out.println(result));
  });
});
```

### AsyncSet

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

CopycatConfig config = new CopycatConfig()
  .withClusterConfig(cluster)
  .addSetConfig("test-set", new AsyncSetConfig()
    .withConsistency(Consistency.STRONG));

Copycat.copycat("tcp://123.456.789.0", config).open()
  .thenApply(copycat -> copycat.<String>set("test-set"))
  .thenCompose(set -> set.open())
  .thenAccept(set -> {
    set.add("Hello world!").thenRun(() -> {
      set.get(0).thenAccept(result -> System.out.println(result));
    });
  });
```

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

AsyncSetConfig config = new AsyncSetConfig()
  .withConsistency(Consistency.STRONG);

AsyncSet.<String>create("tcp://123.456.789.0", cluster, config).open().thenAccept(set -> {
  set.add("Hello world!").thenRun(() -> {
    set.get(0).thenAccept(result -> System.out.println(result));
  });
});
```

### AsyncMultiMap

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

CopycatConfig config = new CopycatConfig()
  .withClusterConfig(cluster)
  .addMultiMapConfig("test-multimap", new AsyncMultiMapConfig()
    .withConsistency(Consistency.STRONG));

Copycat.copycat("tcp://123.456.789.0", config).open()
  .thenApply(copycat -> copycat.<String, String>multiMap("test-multimap"))
  .thenCompose(multiMap -> multiMap.open())
  .thenAccept(multiMap -> {
    multiMap.put("foo", "Hello world!").thenRun(() -> {
      multiMap.get("foo").thenAccept(result -> System.out.println(result));
    });
  });
```

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

AsyncMultiMapConfig config = new AsyncMultiMapConfig()
  .withConsistency(Consistency.STRONG);

AsyncMultiMap.<String, String>create("tcp://123.456.789.0", cluster, config).open().thenAccept(multiMap -> {
  multiMap.put("foo", "Hello world!").thenRun(() -> {
    multiMap.get("foo").thenAccept(result -> System.out.println(result));
  });
});
```

### AsyncLock

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

CopycatConfig config = new CopycatConfig()
  .withClusterConfig(cluster)
  .addLockConfig("test-lock", new AsyncLockConfig()
    .withConsistency(Consistency.STRONG));

Copycat.copycat("tcp://123.456.789.0", config).open()
  .thenApply(copycat -> copycat.lock("test-lock"))
  .thenCompose(lock -> lock.open())
  .thenAccept(lock -> {
    lock.lock().thenRun(() -> {
      System.out.println("Lock locked");
      lock.unlock().thenRun(() -> System.out.println("Lock unlocked");
    });
  });
```

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0", "tcp://123.456.789.1", "tcp://123.456.789.2");

AsyncLockConfig config = new AsyncLockConfig()
  .withConsistency(Consistency.STRONG);

AsyncLock.create("tcp://123.456.789.0", cluster, config).open().thenAccept(lock -> {
  lock.lock().thenRun(() -> {
    System.out.println("Lock locked");
    lock.unlock().thenRun(() -> System.out.println("Lock unlocked");
  });
});
```

## The Copycat cluster

The Copycat cluster is designed not only to support leader election and replication for Copycat and its resources, but
it also serves as a general messaging utility for users as well.

### Members

The Copycat cluster consists of two different types of members, [active](#active) and [passive](#passive). Copycat's
resource behavior differs on each member of the cluster according to the member type. Additionally, each Copycat
instance maintains several versions of the cluster configuration - one global version and a cluster for each partition
of each resource. This allows for a flexible replication model where different resources within the same Copycat
cluster operate completely independently from each other in terms of leader election and log replication.

### Active members

The core of the Copycat cluster consists of a set of *active members*. Active members are required, permanent, voting
members of the Copycat cluster. Active members are defined for each Copycat instance prior to startup:

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0:5000", "tcp://123.456.789.1:5000", "tcp://123.456.789.2:5000");

CopycatConfig config = new CopycatConfig()
  .withClusterConfig(cluster)
  .addResourceConfig(...);

Copycat copycat = Copycat.create("tcp://123.456.789.0:5000", config);
```

By creating a Copycat instance with a URI that is defined as an active member in the cluster configuration, Copycat
knows that this is an active member of the cluster:

```java
assert copycat.cluster().member().type() == Member.Type.ACTIVE;
```

It is important that *all members of the Copycat cluster contain identical cluster configurations*. Note that this
cluster configuration identifies *only* the core active members of the Copycat cluster, and each cluster configuration
must specify at least one active cluster member. Members that are not represented in the cluster configuration are
known as [passive members](#passive-members).

Active members are distinguished from passive members in that they fully participate in log replication via the
[Raft consensus protocol](https://raftconsensus.github.io/). This means that only active cluster members can become
leader, and only active members can serve as synchronous replicas to cluster resources.

Since each Copycat resource (and partition) contains its own separate `Cluster`, Copycat allows the resource's active
membership to differ from the global Copycat cluster membership. This means that even though the Copycat cluster may
consist of five nodes, a resource can perform synchronous replication on only three of those nodes. In that case, the
resource's Raft algorithm would run on those three nodes, and an asynchronous gossip protocol would perform replication
to the remainder of the resource's cluster.

### Passive members

Passive members are members of the Copycat cluster that do not participate in synchronous replication via the Raft
consensus protocol. Instead, Copycat uses a simple gossip protocol to asynchronously replicate committed log entries
to passive members of the cluster.

Members are defined as nodes which are not represented in the Copycat cluster configuration. Thus, starting a `Copycat`
instance or any resource with a local member URI that is not contained within the provided `ClusterConfig` indicates
that the member is passive and should thus receive replicated logs passively:

```java
ClusterConfig cluster = new ClusterConfig()
  .withProtocol(new NettyTcpProtocol())
  .withMembers("tcp://123.456.789.0:5000", "tcp://123.456.789.1:5000", "tcp://123.456.789.2:5000");

CopycatConfig config = new CopycatConfig()
  .withClusterConfig(cluster)
  .addResourceConfig(...);

Copycat copycat = Copycat.create("tcp://123.456.789.4:5000", config);
```

By simply passing a URI that is not defined as an active member of the Copycat cluster, the Copycat instance becomes
a passive member of the cluster:

```java
assert copycat.cluster().member().type() == Member.Type.PASSIVE;
```

See below for more information on
[eventual consistency and Copycat's gossip protocol](#eventual-consistency-and-copycats-gossip-protocol).

### Member states

Throughout the lifecycle of a Copycat cluster, passive members can join and leave the cluster at will. In the case of
passive members, Copycat does not distinguish between a member leaving the cluster and a member crashing. From the
perspective of any given member in the cluster, each other member of the cluster can be in one of three states at any
given time. These three states are defined by the `Member.State` enum:
* `ALIVE` - The member is alive and available for messaging and replication
* `SUSPICIOUS` - The member is unreachable and may have crashed or left the cluster voluntarily
* `DEAD` - The member is no longer part of the cluster

Just as cluster membership can differ across resources, so to can individual member states. Each member within a
resource cluster is guaranteed to also be present in the global Copycat cluster, but members in the Copycat cluster
may not be present in each resource cluster. This is because some passive members of the Copycat cluster may not have
opened all resources. Only once a resource is opened on a passive member of the cluster will it join the resource's
cluster and participate in asynchronous replication via the
[gossip protocol](#eventual-consistency-and-copycats-gossip-protocol).

### Leader election

Leader election within the Copycat cluster is performed on a global and per-resource basis. Each Copycat cluster -
including the global cluster and per-resource clusters - maintains separate leader election state. Copycat provides
a simple API for listening for election events throughout the Copycat cluster.

To register an election listener for the entire Copycat cluster, use the `Copycat` instance's `Cluster`:

```java
copycat.cluster().election().addListener(result -> {
  System.out.println(result.winner() + " elected leader!");
});
```

Additionally, each resource contains the same method for accessing the resource specific cluster's election:

```java
copycat.stateMachine("my-state-machine").open().thenAccept(stateMachine -> {
  stateMachine.cluster().election().addListener(result -> {
    System.out.println(result.winner() + " elected leader!");
  });
});
```

Note, however, that for partitioned resources ([state log](#state-logs) and [event log](#event-logs)), because logs
are created and replicated on a per-partition basis, so are elections performed on a per-partition basis.

```java
eventLog.partition(1).cluster().election().addListener(result -> {
  System.out.println(result.winner() + " elected leader!");
});
```

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

## Protocols

Copycat's communication system provides a pluggable framework that allows the underlying message transport to be
configured based on the environment in which Copycat is deployed. Copycat provides a number of existing protocol
implementations for various asynchronous frameworks.

### The local protocol

The local protocol is a special protocol that is implemented purely for testing purposes. It supports passing messages
across threads via a `ConcurrentHashMap` member registry.

### Netty protocol

The Netty protocol is a fast [Netty](http://netty.io) based TCP protocol.

### Vert.x protocol

The Vert.x protocol module provides several protocol implementations for [Vert.x 2](http://vertx.io), including:
* `VertxEventBusProtocol`
* `VertxTcpProtocol`
* `VertxHttpProtocol`

### Vert.x 3 protocol

Teh Vert.x 3 protocol module provides an event bus protocol implementation for [Vert.x 3](http://vertx.io)

## Architecture

### Strong consistency and Copycat's Raft consensus protocol

#### Leader election

#### Command replication

#### Query consistency

#### Log compaction

### Eventual consistency and Copycat's gossip protocol

#### Log replication

#### Failure detection

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

### [User Manual](#user-manual)
