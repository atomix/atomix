Mimeo
=====

Mimeo is a replication framework for Vert.x. It supports state machine replication
using the Raft consensus algorithm as described in
[this paper](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

**This project is still very much under development and is recommended for testing only**

#### Features
* Automatically replicates state across multiple Vert.x instances in a consistent and reliable manner
* Supports runtime cluster membership changes
* Supports log persistence in MongoDB, Redis, or in memory
* Provides an API for periodic log cleansing
* Tools for automatically detecting cluster members
* Built-in API for executing state machine commands via the Vert.x event bus
* Uses adaptive failure detection for fast leader re-election

### A brief example
Replicated state machines can be used to solve a number of problems. As a simple
example (until I write more documentation), I will just demonstrate how we can write
a fault-tolerant in-memory key-value store (like Redis) with Vert.x and Mimeo.

First, we need to create a new `Vimeo` instance. The `Vimeo` class is the primary API
for Vimeo.
```java
public class MyVerticle extends Verticle {
  @Override
  public void start() {
    Vimeo vimeo = new Vimeo(this);
  }
}
```

With the `Vimeo` instance, we can create a new `Replica` instance.
```java
Replica replica = vimeo.createReplica("test.1", "test");
```

The first argument to `createReplica()` is the event bus address for the local replica.
This must be unique across all Vert.x instances. The second argument can be a cluster
address. The cluster address can be used to send messages to the service, but we'll get
to that later.

Each Vimeo replica contains its own state machine. In Vimeo, state machines are represented
as a set of command handlers registered on the replication service. Each command handler
is a simple function that receives a `Command` as an input and returns a `JsonObject` as
an output. Ultimately, the `JsonObject` is what will be returned to the client.

Let's set up a simple map for our key-value store.

```java
Map<String, Object> store = new HashMap<>();
```

First we'll create a simple `get` command that reads from the data store.

```java
replica.service().registerCommand("get", Command.Type.READ, new Function<Command, JsonObject>() {
  public JsonObject call(Command command) {
    String key = command.data().getString("key");
    return new JsonObject().putValue("result", store.get(key));
  }
});
```

Note that when registering a state machine command we have to pass a `Command.Type` to
the `registerCommand` method. The command type helps tell Vimeo how to handle the command.
`READ` commands are not persisted in the log since they have no impact on the state
of the state machine. On the other hand, `WRITE` and `READ_WRITE` commands **are** persisted
in the log since they alter the state of the state machine. Thus, it is very important
that command types be accurate.

Let's register a `put` command for setting data in the key-value store.

```
replica.service().registerCommand("put", Command.Type.READ, new Function<Command, JsonObject>() {
  public JsonObject call(Command command) {
    String key = command.data().getString("key");
    Object value = command.data().getValue("value");
    store.put(key, value);
    return new JsonObject().putBoolean("result", true);
  }
});
```

And we can register a `del` command for good measure.

```
replica.service().registerCommand("del", Command.Type.READ, new Function<Command, JsonObject>() {
  public JsonObject call(Command command) {
    String key = command.data().getString("key");
    if (store.containsKey(key)) {
      store.remove(key);
      return new JsonObject().putBoolean("result", true);
    }
    return new JsonObject().putBoolean("result", false);
  }
});
```

Note that all command implementations on each node *must* be the same, and they
must each result in the same state and output given the same input in the same
order.

Time to start the replica, but first a little explanation of how it works:

When multiple replicas are started, Vimeo uses the Raft consensus algorithm to
elect a leader. Once a leader has been elected, all commands submitted to the
Vimeo cluster - either via the event bus or the `ReplicationService` API - are
directed to the newly elected leader. When the leader receives a command, it
immediately logs the command and then replicates the new log entry to at least
a majority of the cluster. Once the logged command has been replicated to a
majority of the cluster, the command is applied to the leader's state machine
and the state machine output is returned to the caller. At some point after the
result has been returned, Vimeo will notify the cluster that the entry has been
committed, and cluster members will apply the entry to their own state machines.

If a follower (non-leader) in the cluster dies, its log will have been persisted
on the local machine. Even if the replica was not using a persistent log, once the
replica is restarted and rejoins the cluster, the leader of the cluster will again
replicate its log to the newly restarted replica and get its state back to consistency
with the other nodes. Alternatively, if the cluster leader dies then the failure
will be automatically detected by the other nodes which will start a new election
and determine a new leader by consensus. The new leader is determined based on how
up-to-date each replica's state is.

To start the replica we simply call the `start` method.

```java
replica.start(new Handler<AsyncResult<Void>>() {
  public void handle(AsyncResult<Void> result) {
    if (result.succeeded()) {
      
    }
  }
});
```

Once the replica has been started we can send a command to it via the Vert.x
event bus using the cluster address that we set up earlier, `test`.

```java
JsonObject command = new JsonObject()
  .putString("command", "put")
  .putString("key", "foo")
  .putValue("value", "Hello world!");
vertx.eventBus().send("test", command, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    JsonObject command = new JsonObject()
      .putString("command", "get")
      .putString("key", "foo");
    vertx.eventBus().send("test", command, new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> message) {
        String result = message.body().getString("result");
        System.out.println(result); // Hello world!
      }
    });
  }
})
```

Note that this was only an example of a single node cluster, and that's not very
useful for replication. To set up cluster membership, we can add replica addresses
to the cluster configuration prior to starting the replica.

```java
replica.cluster().addMember("test.2");
replica.cluster().addMember("test.3");
```

By default, Vimeo's cluster membership is static, meaning cluster membership is
indicated only by method calls on the cluster controller. But Vimeo can also use
the cluster address to automatically detect cluster members by enabling dynamic
cluster membership.

```java
replica.cluster().enableDynamicMembership();
```

With dynamic cluster membership, when the replica is started, Vimeo will broadcast
a message to the cluster searching for other members within the same cluster. Since
Vimeo supports dynamic cluster configuration changes for the Raft algorithm, cluster
members can be added and removed from the cluster at runtime by simply starting and
stopping Mimeo replicas.

### The full example
Finally, here is the full code for a simple fault-tolerant key-value store using Vimeo.

```java
public class MyVerticle extends Verticle {
  private Mimeo mimeo;
  private final Map<String, Object> store = new HashMap<>();

  @Override
  public void start() {
    mimeo = new Mimeo(this);

    // Create and start three replicas and configure each replica for dynamic cluster membership.
    final Replica replica1 = createReplica("test.1", "test");
    replica1.start(new Handler<AsyncResult<Void>>() {
      public void handle(AsyncResult<Void> result) {
        if (result.succeeded()) {
        final Replica replica2 = createReplica("test.2", "test");
          replica2.start(new Handler<AsyncResult<Void>>() {
            public void handle(AsyncResult<Void> result) {
              if (result.succeeded()) {
                final Replica replica3 = createReplica("test.3", "test");
                replica3.start(new Handler<AsyncResult<Void>>() {
                  public void handle(AsyncResult<Void> result) {
                    if (result.succeeded()) {
                      sendCommands();
                    }
                  }
                });
              }
            }
          });
        }
      }
    });
  }

  private void sendCommands() {
    // Put a new value in the key-value store.
    JsonObject command = new JsonObject()
      .putString("command", "put")
      .putString("key", "test")
      .putValue("value", "Hello world!");

    vertx.eventBus().send("test", command, new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> message) {
        System.out.println(message.body().getBoolean("result")); // true

        // Get the value back out of the key-value store.
        JsonObject command = new JsonObject()
          .putString("comamnd", "get")
          .putString("key", "test");

        vertx.eventBus().send("test", command, new Handler<Message<JsonObject>>() {
          public void handle(Message<JsonObject> message) {
            System.out.println(message.body().getString("result")); // Hello world!

            // Delete the value from the key-value store.
            JsonObject command = new JsonObject()
              .putString("command", "del")
              .putString("key", "test");

            vertx.eventBus().send("test", command, new Handler<Message<JsonObject>>() {
              public void handle(Message<JsonObject> result) {
                System.out.println(message.body().getBoolean("result")); // true
              }
            });
          }
        });
      }
    });
  }

  private Replica createReplica(String address, String cluster) {
    Replica replica = mimeo.createReplica(address, cluster);
    replica.cluster().enableDynamicMembership();

    // Register a get command.
    replica.service().registerCommand("get", Command.Type.READ, new Function<Command, JsonObject>() {
      public JsonObject call(Command command) {
        String key = command.data().getString("key");
        return new JsonObject().putValue("result", store.get(key));
      }
    });

    // Register a put command.
    replica.service().registerCommand("put", Command.Type.READ, new Function<Command, JsonObject>() {
      public JsonObject call(Command command) {
        String key = command.data().getString("key");
        Object value = command.data().getValue("value");
        store.put(key, value);
        return new JsonObject().putBoolean("result", true);
      }
    });

    // Register a delete command.
    replica.service().registerCommand("del", Command.Type.READ, new Function<Command, JsonObject>() {
      public JsonObject call(Command command) {
        String key = command.data().getString("key");
        if (store.containsKey(key)) {
          store.remove(key);
          return new JsonObject().putBoolean("result", true);
        }
        return new JsonObject().putBoolean("result", false);
      }
    });
  }

}
```
