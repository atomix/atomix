CopyCat
=======
CopyCat is a fault-tolerant state machine replication framework built on Vert.x. It
provides a simple and flexible API built around the Raft consensus algorithm as
described in [the excellent paper by Diego Ongaro and John Ousterhout](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

**This project is experimental and is not recommended for production**

Right now this project is purely academic. I'm making it public in the hopes of
gaining intrest and contributions from the open-source community. Pull requests welcome!

## Table of contents
1. [Features](#features)
1. [How it works](#how-it-works)
1. [Creating a replica](#creating-a-replica)
1. [Setting up the cluster](#setting-up-the-cluster)
1. [Registering state machine commands](#registering-state-machine-commands)
1. [Submitting commands](#submitting-commands)
1. [Working with snapshots](#working-with-snapshots)
1. [The CopyCat context](#the-copycat-context)
1. [Managing the cluster](#managing-the-cluster)

### Features

## How it works

### Creating a replica
For most use cases, users will use the `CopyCat` class to create a CopyCat replica.
CopyCat clusters are made of one or more instances of the `CopyCat` class. Each
`CopyCat` instance contains a `ClusterConfig` which defines the replica's position
within the cluster. CopyCat uses this cluster information to communicate with other
nodes in the cluster to perform leader election and log replication.

```java
CopyCat copycat = new CopyCat(vertx);
```

The `CopyCat` constructor accepts a number of arguments. Most important of these are
the `ClusterConfig` and `Log`.

CopyCat provides two different log implementations: an in-memory log and a file-based
log. By default, the `CopyCat` class uses a `MemoryLog` for simplicity. To use a `FileLog`
simply pass a `FileLog` instance to the constructor.

```java
CopyCat copycat = new CopyCat(vertx, new FileLog("foo.dat"));
```

To start the `CopyCat` instance, simply call the `start` method.

```java
copycat.start();
```

The replica will be started asynchronously. Once the replica has identified a cluster
leader or has itself been elected leader, it will call the optional `AsyncResult` handler.

```java
copycat.start(Handler<AsyncResult<Void>>() {
  public void handle(AsyncResult<Void> result) {
    if (result.succeeded()) {
      // A leader has been elected
    }
  }
});
```

### Setting up the cluster
In order for your `CopyCat` instance to know how to communicate with other nodes in
the cluster, it must know its position within a cluster. CopyCat nodes are identified
by unique event bus addresses. To assign an event bus address to a node, create a
`ClusterConfig`.

```java
ClusterConfig cluster = new StaticClusterConfig("foo");
CopyCat copycat = new CopyCat(vertx, cluster);
```

The first argument to the cluster configuration will always be the local member address.
However, in order for the local node to communicate with other nodes in the cluster, you
must identify those other nodes through the cluster configuration.

```java
cluster.addRemoteMember("bar");
cluster.addRemoteMember("baz"):
```

CopyCat provides a `StaticClusterConfig` and `DynamicClusterConfig` for static and dynamic
clusters respectively. The only real difference between these two configuration classes is
that the `DynamicClusterConfig` is an `Observable` type. This allows the cluster's configuration
to be dynamically changed at runtime.

The configuration change process is actually a two-phase process that ensures
that consistency remains intact during the transition. When the leader receives
a configuration change, it actually replicates two separate log entries - one
with the combined old and new configuration, and - once that configuration
has been replicated and committed - the final updated configuration. This
ensures that split majorities cannot occur during the transition.

CopyCat does provide facilities for automatically detecting cluster membership. More on
that later.

### Registering state machine commands
A state machine is a model of a machine whose state changes over time.
Multiple instances of the same state machine will always arrive at the
same state given the same set of commands in the same order. This means
it is important that all state machines behave the same way. When copy cat
replicates application state, it's actually replicating a log of commands
which will be applied to each state machine once they've been reliably
replicated to the majority of the cluster.

`CopyCat` instances are replicated state machines. In order for log replication and command
execution to take place, each instance must have a set of registered state machine commands.
Commands should be identical on each node in the cluster.

Java commands are defined by implementing the `Command<JsonObject, JsonObject>` interface.
Each command is assigned a unique name by which it can be referenced when executing commands.
To register a command on a `CopyCat` instance call the `registerCommand` method.

```java
final Map<String, Object> data = new HashMap<>();

copycat.registerCommand("set", new Command<JsonObject, JsonObject>() {
  @Override
  public JsonObject execute(JsonObject args) {
    String key = args.getString("key");
    Object value = args.getValue("value");
    data.put(key, value);
    return null;
  }
});
```

In this case, we register a write command that writes a key and value to an internal map.
Alternatively, we can register a command to read data from the same map.

```java
final Map<String, Object> data = new HashMap<>();

copycat.registerCommand("get", new Command<JsonObject, JsonObject>() {
  @Override
  public JsonObject execute(JsonObject args) {
    String key = args.getString("key");
    return new JsonObject().putValue("value", data.get(key));
  }
});
```

When registering read-only commands like the one above, it is useful to register the command
using a command type. By registering the command as a `READ` command, CopyCat can take measures
to improve performance by removing the need to log and replicate the command. This makes reads
potentially much faster than writes.

```java
copycat.registerCommand("get", CommandInfo.Type.READ, new Command<JsonObject, JsonObject>() ...);
// or
copycat.registerReadCommand("get", new Command<JsonObject, JsonObject>() ...);
```

### Submitting commands
Once commands have been registered for the state machine and the `CopyCat` instance has been
started, we can submit commands to be evaluated by the cluster. To submit a command to the
CopyCat cluster use the `submitCommand` method.

```java
JsonObject data = new JsonObject()
  .putString("key", "foo")
  .putString("value", "Hello world!");
copycat.submitCommand("set", data, new Handler<AsyncResult<JsonObject>>() {
  @Override
  public void handle(AsyncResult<JsonObject> result) {
    if (result.succeeded()) {
      // Successfully wrote the value.
    }
  }
});
```

When the command is submitted to the cluster, it will first be forwarded to the currently
elected leader. If no leader is currently available then the command will be queued. If the
command queue fills up then the command will fail. If a leader does exist, once the leader
receives the command submission, it will do the following:
* If the command is a `READ` command, the leader will ensure the replicated log is in sync
  and apply the command to its state machine, returning the result
* If the command is a `WRITE` or `READ_WRITE` command, the leader will log the command, replicate
  the entry to its followers, and once the entry has been committed (replicated to a quorum
  of the cluster) apply the command to its state machine and return the result

### Working with snapshots
In order to support log compaction, CopyCat provides facilities for creating and installing
state machine snapshots. When the local log becomes too full, CopyCat will automatically
attempt to take a snapshot of the machine state by calling the `SnapshotCreator` if one
exists. Alternatively, when the state machine is first started, it will install any
persisted snapshots using the registered `SnapshotInstaller`. Snapshots are persisted to
the local log but are not replicated. Once a snapshot has been persisted to the log, all
log entries prior to the snapshot entry will be purged from the log.

To support snapshots in your state machine, register a `SnapshotCreator` and `SnapshotInstaller`.

```java
copycat.snapshotCreator(new SnapshotCreator() {
  @Override
  public JsonObject createSnapshot() {
    return new JsonObject(data);
  }
});

copycat.snapshotInstaller(new SnapshotInstaller() {
  @Override
  public void installSnapshot(JsonObject snapshot) {
    data = snapshot.toMap();
  }
});
```

### The CopyCat context
Underlying each `CopyCat` instance is a `CopyCatContext` which handles the internal replica
state. This API is hidden because it is primarily only useful in the Java context, but users
can use it as an alternative to the `CopyCat` class. The primary difference is that a
`StateMachine` instance must be provided when using the `CopyCatContext`.

```java
public class MyStateMachine implements StateMachine {

  @Override
  public JsonObject applyCommand(String command, JsonObject args) {
    switch (command) {
      case "get":
        return get(args);
      case "set":
        return set(args);
    }
  }

}
```

If you want to provide types for commands in a `StateMachine` implementation, implement
the `CommandProvider` interface in order to provide `CommandInfo` to CopyCat.

```java
public class MyStateMachine implements StateMachine, CommandProvider {

  @Override
  public CommandInfo getCommandInfo(String name) {
    if (name.equals("get")) {
      return new GenericCommandInfo("get", CommandInfo.Type.READ);
    }
    return null;
  }

}
```

Similarly, the `StateMachine` can support snapshots by implementing the `SnapshotCreator`
and `SnapshotInstaller` interfaces just as before.

```java
public class MyStateMachine implements StateMachine, SnapshotCreator, SnapshotInstaller {
  private Map<String, Object> data = new HashMap<>();

  @Override
  public JsonObject createSnapshot() {
    return new JsonObject(data);
  }

  @Override
  public void installSnapshot(JsonObject snapshot) {
    this.data = snapshot.toMap();
  }

}
```

### Managing the cluster
CopyCat provides facilities for managing cluster membership using Vert.x features. CopyCat
can detect cluster members through either Vert.x shared data or through the Vert.x
Hazelcast cluster. Cluster membership detection is done by a `ClusterManager`.

To use a `ClusterManager` with a `CopyCat` instance, simply pass a `ClusterManager` instance
rather than a `ClusterConfig` to the `CopyCat` constructor.

```java
CopyCat copycat = new CopyCat(vertx, new HazelcastClusterManager("test", vertx));
```

Cluster manager constructors generally require a cluster name with which they can coordinate
cluster membership. What this means varies depending on the cluster manager type. CopyCat
provides several cluster manager implementations:
* `StaticClusterManager` - a placeholder cluster manager which does not change cluster membership
* `LocalClusterManager` - a cluster manager implemented on Vert.x shared data and the event bus.
  The local cluster manager can detect cluster membership changes within the current Vert.x instance.
* `HazelcastClusterManager` - a Vert.x Hazelcast cluster based cluster manager. The Hazelcast cluster
  manager uses Hazelcast data structures and events to detect cluster membership changes.

Cluster managers remove the need for managing the cluster configuration yourself. So, if you
want to cluster several `CopyCat` instances within a single Vert.x instance, all you need to
do is create each instance with a `LocalClusterManager` using the same name.

```java
CopyCat copycat1 = new CopyCat(vertx, new LocalClusterManager("cluster", vertx)).start();
CopyCat copycat1 = new CopyCat(vertx, new LocalClusterManager("cluster", vertx)).start();
CopyCat copycat1 = new CopyCat(vertx, new LocalClusterManager("cluster", vertx)).start();
```

These three `CopyCat` instances will be automatically clustered with one another.

#### Issues with cluster management
Dynamic cluster membership changes are a risky proposition when using the Raft algorithm.
CopyCat takes measures to ensure that logs do not become out of sync during configuration
changes by incrementally committing cluster configuration changes to the replicated log.
But when dynamic cluster detection is used, network partitions can still result in logs
becoming out of sync. This is because the cluster manager may simply assume that the network
has been reconfigured and thus commit the reconfiguration to the CopyCat log. However, in
the case that the network has not been reconfigured and a partition occurred, each side of
the partition will continue to accept writes, and once the partition is resolved, one log
will overwrite the other.

In order to prevent this type of issue resulting from network partitions when using cluster
membership detection, it is *strongly recommended* that you specify a `quorumSize` required
for the cluster to operate. To specify a quorum size, pass the size as an additional argument
to the cluster manager.

```java
CopyCat copycat1 = new CopyCat(vertx, new LocalClusterManager("cluster", vertx, 2)).start();
CopyCat copycat1 = new CopyCat(vertx, new LocalClusterManager("cluster", vertx, 2)).start();
CopyCat copycat1 = new CopyCat(vertx, new LocalClusterManager("cluster", vertx, 2)).start();
```

If your cluster consists of at most three nodes, the quorum size should be `2`. If your
cluster consists of at most five nodes, the quorum size should be `3`, and so on...
