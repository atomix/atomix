CopyCat
=======

CopyCat is a fault-tolorant replication framework for Vert.x. It provides
for state machine replication using the Raft consensus algorithm as described in
[this paper](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

**This project is still very much under development and is recommended for testing only**

## Table of contents
1. [Features](#features)
1. [How it works](#how-it-works)
1. [The CopyCat API](#the-copycat-api)
1. [Working with replicas](#working-with-replicas)
   * [Registering commands](#registering-state-machine-commands)
   * [Submitting commands to the replica](#submitting-commands-to-the-replica)
   * [Submitting commands over the event bus](#submitting-commands-over-the-event-bus)
1. [Working with clusters](#working-with-clusters)
   * [Static cluster membership](#static-cluster-membership)
   * [Dynamic cluster membership detection](#dynamic-cluster-membership-detection)
1. [Working with logs](#working-with-logs)
   * [Log types](#log-types)
      * [MemoryLog](#memorylog)
      * [MongoLog](#mongolog)
      * [RedisLog](#redislog)
   * [Log compaction](#log-compaction)
      * [Log cleaning](#log-cleaning)
      * [Log snapshotting](#log-snapshotting)
1. [Putting it all together with services](#copycat-services)
1. [Key-value store in 50 lines of code](#a-simple-key-value-store)

### Features
* Automatically replicates state across multiple Vert.x instances in a consistent and reliable manner
* Supports runtime cluster membership changes
* Supports replicated log persistence in MongoDB, Redis, or in memory
* Provides an API for periodic log cleaning
* Provides tools for automatically detecting cluster members
* Exposes an event bus API for executing state machine commands remotely
* Uses adaptive failure detection for fast leader re-election

## How it works
CopyCat provides tools for creating fault-tolerant Vert.x services by
replicating state across multiple Vert.x instances and coordinating requests
to the service. When multiple CopyCat replicas are started within a Vert.x
cluster, the replicas communicate with each other to elect a leader which
coordinates the cluster and replicates commands to all the nodes in the
cluster. CopyCat's leader election and replication is performed using
a modified implementation of the [Raft](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf)
consensus algorithm. The replication of CopyCat cluster states means that
if any member of the cluster dies, the state is not lost. The cluster will
simply coordinate to share state, and once the dead replica re-joins the
cluster, any commands missed while the node was down will be replicated
to the node.

## The CopyCat API
CopyCat provides a primary factory API for creating various types. To create
CopyCat objects, create a new `CopyCat` instance.

```java
public class MyVerticle extends Verticle {

  public void start() {
    CopyCat factory = new CopyCat(this);
  }

}
```

You can pass a `Vertx` or `Verticle` instance to the constructor

The `CopyCat` type exposes the following factory methods:
* `createReplica()` - Creates a [replica](#working-with-replicas)
* `createReplica(String address)` - Creates a [replica](#working-with-replicas)
* `createReplica(String address, ClusterConfig config)` - Creates a [replica](#working-with-replicas)
* `createCluster()` - Creates a [cluster](#working-with-clusters)
* `createCluster(String localAddress, String broadcastAddress)` - Creates a [cluster](#working-with-clusters)
* `createService()` - Creates a [service](#copycat-services)
* `createService(String address)` - Creates a [service](#copycat-services)
* `createService(String address, Log log)` - Creates a [service](#copycat-services)
* `createEndpoint(String address)` - Creates a [service endpoint](#submitting-commands-over-the-event-bus)

## Working with replicas
CopyCat clusters are made up of any number of `Replica` instances running
on disparate Vert.x instances in different physical locations. CopyCat
provides a simple API for creating replicas as well as a number of other
tools which will be covered later.

For simple access to CopyCat APIs, create a `CopyCat` instance.

```java
public class MyVerticle extends Verticle {

  public void start() {
    CopyCat copycat = new CopyCat(this);
  }

}
```

From there, we can create a replica by simply calling `createReplica`

```java
Replica replica = copycat.createReplica("test.1");
```

We pass a unique address for the replica as the only argument. This is the
address of the replica being deployed. To start the replica, simply call the
`start` method:
* `start()`
* `start(Handler<AsyncResult<Void>> doneHandler)`

### Registering commands
CopyCat state machines are represented as a series of commands that are
serviced by the cluster. Commands are registered by calling the
`registerCommand` method on a `Replica` instance.

```java
replica.registerCommand("set", new Function<Command, Boolean>() {
  public Boolean call(Command command) {
    String key = command.args().getString("key");
    Object value = command.args().getValue("value");
    map.put(key, value);
    return true;
  }
});
```

Commands are simply functions that receive a command object containing
a specific argument type and return output. Commands don't necessarily
have to alter the state of the replica. For instance, a `put` and `delete`
would alter the replica's state, but `get` wouldn't because it is a *read-only*
action. This allows CopyCat to make some performance improvements by ignoring
read-only commands for replication purposes.

To mark a command as *read-only*, pass a `Command.Type` as the second
argument to `registerCommand`

```java
replica.registerCommand("get", Command.Type.READ, new Function<Command, Object>() {
  ...
});
```

### Submitting commands to the replica
With commands registered on the replica, we can start the replica and begin
to submit commands.

```java
JsonObject command = new JsonObject()
    .putString("key", "foo")
    .putString("value", "Hello world!");
replica.submitCommand("set", command, new Handler<AsyncResult<Boolean>>() {
  public void handle(AsyncResult<Boolean> result) {
    if (result.succeeded() && result.result()) {
      // Successfully set "foo" to "Hello world!"
    }
  }
});
```

Now, we can read the store key value back from the replica.

```java
JsonObject command = new JsonObject()
    .putString("key", "foo");
replica.submitCommand("get", command, new Handler<AsyncResult<String>>() {
  public void handle(AsyncResult<String> result) {
    if (result.succeeded()) {
      System.out.println(result.result()); // Hello world!
    }
  }
});
```

As mentioned, in this scenario the `set` command is logged and replicated,
while the `get` command - a *read-only* command - is simply executed
without replication.

### Submitting commands over the event bus
What we've seen so far is a method for submitting commands to the cluster
via the `submitCommand` method. But this is Vert.x, and in Vert.x we expose
APIs over the event bus. CopyCat provides a `ReplicaEndpoint` class for
exposing clusters over the event bus. To create an endpoint call the
`createEndpoint` method on a `CopyCat` instance, passing the cluster
address as the only argument.

```java
ReplicaEndpoint endpoint = copycat.createEndpoint("test");
endpoint.start();
```

In the case of endpoints, all replicas should expose an endpoint *at the
same event bus address*. Vert.x event bus behavior will distribute requests
between each endpoint instance across the cluster.

To submit a command over the event bus, send a `JsonObject` message to
the endpoint address with a `command` and any additional arguments to
the command implementation.

```java
JsonObject command = new JsonObject()
    .putString("command", "get")
    .putString("key", "foo");
vertx.eventBus().send("test", command, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    if (message.body().getString("status").equals("ok")) {
      String result = message.body().getString("result");
    }
  }
});
```

The reply message will contain the following fields:
* `status` - either `ok` or `error`. If the `status` is `ok` then an
additional `result` field will contain the command result. If the status
is `error` then an additional `message` field will contain an error message.

## Working with clusters
We've seen how to create replicas, register state machine commands, and
execute those commands. But replication doesn't do any good without clusters.
In order to replicate state, CopyCat replicas need to have information about
other members within their cluster (other members with whom they should
communicate and replicate). CopyCat provides a couple of methods for
configuring clusters.

### Static cluster membership
The most obvious method of telling a replica about its fellow replicas is
by updating the replica's `ClusterConfig`, the object that represents a
registry of all replicas in the cluster.

```java
replica.cluster().addMember("test.2");
replica.cluster().addMember("test.3");
```

This will tell the replica to participate in leader elections and state
replications with `test.2` and `test.3`. Similarly, when the `test.2`
and `test.3` replicas are deployed, their configurations will need to contain
`test.1`.

CopyCat supports updating the cluster configuration even while replicas
are running. If a cluster configuration changes during normal operation,
the leader of the cluster will begin a two-step process that helps ensure
safety during the transition.

First, the leader will append a `CONFIGURATION` entry to its log. This
first entry will contained a *combined* membership for the cluster. So,
it will contain a list of all the replicas in *both the old and the new*
configuration. Once this combined configuration is replicated to the
rest of the cluster, the leader will append a second `CONFIGURATION`
entry to its log - this one containing only the new configuration -
and replicate that entry. This process helps ensure that two majorities
cannot be created while different parts of the cluster have varying
configurations. Once both phases of the configuration change have been
committed the update is complete.

Note that *even after the replica is deployed you can update the
cluster configuration*. When the cluster configuration is updated while
the replica is running, the new configuration will be logged and replicated
to all the other *known* replicas in the cluster. This means that updating
the configuration on one replica will result in the configuration being
updated on all replicas. The updated configuration will be persisted on all
replicas and thus will remain after failures as well.

CopyCat's support for run-time configuration changes allow it to provide
additional features for detecting clusters in a dynamic framework like
Vert.x.

### Dynamic cluster membership detection
This type of manual cluster configuration may not seem very efficient,
and I agree, so CopyCat also provides tools for automating the detection
of cluster members via the Vert.x event bus. Using a `ClusterController`,
CopyCat will automatically locate members across the cluster.

```java
copycat.createCluster()
  .setLocalAddress("test.1")
  .setBroadcastAddress("test")
  .start(new Handler<AsyncResult<ClusterConfig>>() {
    public void handle(AsyncResult<ClusterConfig> result) {
      if (result.succeeded()) {
        ClusterConfig config = result.result();
        Replica replica = copycat.createReplica("test.1", config);
      }
    }
  });
```

Any time the cluster membership changes (a replica dies), the local replica
will be informed of the membership change and the new configuration will
be replicated across the cluster. Then, once the dead replica comes back online,
the cluster configuration will again be updated and replicated, and
the rest of the cluster state will be replicated to the re-joining replica.

## Working with logs
Behind the scenes, CopyCat uses replicated logs to replica and persist
commands across a cluster. Logs make up the basis for consistent replication
and provide the tools necessary to resolve conflicts between different
cluster members.

### Log types
CopyCat provides several different log implementations, each of which use
existing Vert.x APIs for persistence

#### MemoryLog
The memory log is a completely in-memory log implemented using a `TreeMap`.
This is a very fast log implementation, but its primary drawback is that
logs are not persisted, so each time a replica starts up it must receive
the replicated log over the event bus from another replica in the cluster.
Additionally, memory limits mean the `MemoryLog` is only suitable for
smaller states. Periodic log cleaning can help ensure that the log size
remains small.

#### MongoLog
The Mongo log is a log implementation using Mongo and the
[mod-mongo-persistor](https://github.com/vert-x/mod-mongo-persistor) module.
Given a MongoDB collection, the Mongo log will store log entries as indivual
documents and uses Mongo's flexible operations to sort and remove log
entries as necessary.

#### RedisLog
The Redis log is a log implementation using Redis and the
[mod-redis](https://github.com/vert-x/mod-redis) module. Given a Redis
database, the Redis log stores each log entry as a separate key, using
the base key as a counter (using `INCR`). Often times indiviual requests
must be made for each entry when removing large portions of the log (which
should be a rare operation) so event bus usage may be slightly increased
over the Mongo log implementation.

### Log compaction
Over the long term, replicated logs can grow indefinitely. Obviously
indefinite log growth is not ideal, so users need a way to minimize the
size of logs wherever possible.

#### Log cleaning
One feature we can not about command logs is that often times the logs
will contain commands that no longer contribute to the current state of
the application. For example, a `set` command might override a previous
`set` command, or a `del` command negates the state that was previously
created by a `set` command. In this case, each of the commands can be
safely removed from the log without effecting the system's ability to
recover to its previous state. This is referred to as log cleaning.

Log cleaning is a incremental process that can be performed periodically
or even each time a command is applied to the state machine. When entries
are removed from the log, remaining entries are compacted to the head
of the log.

CopyCat provides a simple API for log cleaning. Command entries can be
freed from the log by simply calling `free()` on any `Command` instance.

```java
// Create a map of keys to commands.
final Map<String, Command> commands = new HashMap<>();

replica.registerCommand("set", new Function<Command, Boolean>() {
  public Boolean call(Command command) {
    String key = command.args().getString("key");
    Object value = command.args().getValue("value");
    store.put(key, value);

    // Update the command for this key. If a previous command exists
    // for this key, the previous command no longer has an effect on
    // the system state, so we can free the command from the log.
    // This helps keep the size of the log minimal.
    if (commands.containsKey(key)) {
      commands.remove(key).free();
    }

    // Add the new command, which is the command that resulted in the current state.
    commands.put(key, command);

    return true;
  }
}); 
```

#### Log snapshotting
While log cleaning is a fast and incremental process that is ideal for
many situations, it can often require complex logic to determine whether
any given command contributes to the state of the state machine. A much
simpler method of compacting the log is to periodically take snapshots
of the current system state and then wipe the log completely. If the
replica fails and then restarts, it will first be initialized with the
persisted state snapshot before applying proceeding log entries.

## CopyCat Services
So far, we've demonstrated a number of separate tools CopyCat provides for
cluster detenction, replication, and communication. It's obvious to me
that it would be awesome if this were all put into a single simple API,
so I did that by combining all the functionality into a CopyCat `CopyCatService`.

To create a new `CopyCatService` simply call the `createService` method on a
`CopyCat` instance.

```java
CopyCatService service = copycat.createService("test");
```

Here we pass the service address as the only argument. This is the address
that will be exposed by the `ReplicaEndpoint` on the Vert.x event bus.
Services operate much like replicas. Indeed, they expose the same API as
replicas aside from the `setBroadcastAddress(String address)` method, which
in most cases should not even be used since CopyCat will create a consistent
cluster broadcast address internally. So, to start the service we simply
register our commands and call the `start` method as with the `Replica`.

```java
service.registerCommand("set", new Function<Command, Boolean>() {
  ...
});
service.registerCommand("get", Command.Type.READ, new Function<Command, Object>() {
  ...
});
service.registerCommand("del", new Function<Command, Boolean>() {
  ...
});
service.start();
```

CopyCat will handle cluster membership detection and configuration internally,
as well as expose the service address - in this case `test` - and all the
service commands over the Vert.x event bus. When multiple instances of the
service are started they form a fault-tolerant service.

## A Simple Key-Value Store
To demonstrate the tools that CopyCat provides, this is a simply example
of a Redis-style fault-tolerant in-memory key-value store exposed over
the Vert.x event bus.

```java
public class KeyValueStore extends Verticle {
  // A map of key-value pairs.
  private final Map<String, Object> store = new HashMap<>();

  // A map of keys and the commands which effect their state.
  private final Map<String, Command> commands = new HashMap<>();

  @Override
  public void start(final Future<Void> startedResult) {
    // The key-value store service address.
    final String address = container.config().getString("address", "key-value");

    // Create a CopyCat Service instance.
    final CopyCat copycat = new CopyCat(this);
    final CopyCatService service = copycat.createService(address);

    // Register service commands.

    // Register a "set" command.
    service.registerCommand("set", new Function<Command, Boolean>() {
      public Boolean call(Command command) {
        String key = command.args().getString("key");
        Object value = command.args().getValue("value");
        store.put(key, value);

        // Update the command for this key. If a previous command exists
        // for this key, the previous command no longer has an effect on
        // the system state, so we can free the command from the log.
        // This helps keep the size of the log minimal.
        if (commands.containsKey(key)) {
          commands.remove(key).free();
        }

        // Add the new command, which is the command that resulted in the current state.
        commands.put(key, command);

        return true;
      }
    });

    // Register a "get" command.
    service.registerCommand("get", Command.Type.READ, new Function<Command, Object>() {
      public Object call(Command command) {
        String key = command.args().getString("key");
        return store.get(key);
      }
    });

    // Register a "del" command.
    service.registerCommand("del", new Function<Command, Boolean>() {
      public Boolean call(Command command) {
        String key = command.args().getString("key");
        if (store.containsKey(key)) {
          store.remove(key);

          // Remove all commands related to this key. Since the key has
          // been removed from the data store, no commands related to
          // the key are necessary as the key no longer has a state.
          if (commands.containsKey(key)) {
            commands.remove(key).free();
          }
          return true;
        }
        return false;
      }
    });

    // Start the service.
    service.start(new Handler<AsyncResult<Void>>() {
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          KeyValueStore.super.start(future);
        }
      }
    });
  }

}
```

This example demonstrates a fault-tolerant key-value store. To start the
store, all we need to do is simply run the verticle in cluster mode, passing
an optional `address` configuration if desired.

```
vertx run KeyValueStore.java -cluster -cluster-host ...
```

We can start as many instances of the data store as we want. Replicas will
automatically detect and replicate state (the key-value store) to one another.
To execute a service command, we simply use the API exposed on the event
bus.

```java
// Create a set command
JsonObject command = new JsonObject()
  .putString("command", "set")
  .putString("key", "foo")
  .putString("value", "Hello world!");

// Set "foo" to "Hello world!"
vertx.eventBus().send("key-value", command, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    if (message.body().getString("status").equals("ok")) {

      // Create a get command
      JsonObject command = new JsonObject()
        .putString("command", "get")
        .putString("key", "foo");

      // Get the "foo" key
      vertx.eventBus().send("key-value", command, new Handler<Message<JsonObject>>() {
        public void handle(Message<JsonObject> message) {
          if (message.body().getString("status").equals("ok")) {
            String value = message.body().getString("result");
            System.out.println(value); // Hello world!
          }
        }
      });
    }
  }
});
```
