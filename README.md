CopyCat
=======

CopyCat is a fault-tolorant replication framework for Vert.x. It provides
for state machine replication using the Raft consensus algorithm as described in
[this paper](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

**This project is still very much under development and is recommended for testing only**

#### Features
* Automatically replicates state across multiple Vert.x instances in a consistent and reliable manner
* Supports runtime cluster membership changes
* Supports replicated log persistence in MongoDB, Redis, or in memory
* Provides an API for periodic log cleaning
* Provides tools for automatically detecting cluster members
* Exposes an event bus API for executing state machine commands remotely
* Uses adaptive failure detection for fast leader re-election

### How it works
CopyCat provides tools for creating fault-tolerant Vert.x services by
replicating state across multiple Vert.x instances and coordinating requests
to the service. When multiple CopyCat nodes are started within a Vert.x
cluster, the nodes communicate with each other to elect a leader which
coordinates the cluster and replicates commands to all the nodes in the
cluster. CopyCat's leader election and replication is performed using
a modified implementation of the [Raft](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf)
consensus algorithm. The replication of CopyCat cluster states means that
if any member of the cluster dies, the state is not lost. The cluster will
simply coordinate to share state, and once the dead node re-joins the
cluster, any commands missed while the node was down will be replicated
to the node.

### A Simple Key-Value Store
To demonstrate the tools that CopyCat provides, this is a simply example
of a Redis-style fault-tolerant in-memory key-value store exposed over
the Vert.x event bus.

```java
public class KeyValueStore extends Verticle {
  // A map of key-value pairs.
  private final Map<String, Object> store = new HashMap<>();

  // A map of keys and the commands which effect their state.
  private final Map<String, Command<?>> commands = new HashMap<>();

  @Override
  public void start(final Future<Void> startedResult) {
    // The key-value store service address.
    final String address = container.config().getString("address", "key-value");

    // Create a CopyCat Service instance.
    final CopyCat copycat = new CopyCat(this);
    final CopyCatService service = copycat.createService(address);

    // Register service commands.

    // Register a "set" command.
    service.registerCommand("set", new Function<Command<JsonObject>, Boolean>() {
      public Boolean call(Command<JsonObject> command) {
        String key = command.data().getString("key");
        Object value = command.data().getValue("value");
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
    service.registerCommand("get", Command.Type.READ, new Function<Command<JsonObject>, Object>() {
      public Object call(Command<JsonObject> command) {
        String key = command.data().getString("key");
        return store.get(key);
      }
    });

    // Register a "del" command.
    service.registerCommand("del", new Function<Command<JsonObject>, Boolean>() {
      public Boolean call(Command<JsonObject> command) {
        String key = command.data().getString("key");
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

We can start as many instances of the data store as we want. Nodes will
automatically detect and replicate state (the key-value store) to one another.
To execute a service command, we simply use the API exposed on the event
bus.

```java
// Create a set command
JsonObject command = new JsonObject()
  .putString("command", "set")
  .putObject("data", new JsonObject()
    .putString("key", "foo")
    .putString("value", "Hello world!"));

// Set "foo" to "Hello world!"
vertx.eventBus().send("key-value", command, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    if (message.body().getString("status").equals("ok")) {

      // Create a get command
      JsonObject command = new JsonObject()
        .putString("command", "get")
        .putObject("data", new JsonObject().putString("key", "foo"));

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

### Creating a CopyCat cluster
CopyCat clusters are made up of any number of `CopyCatNode` instances running
on disparate Vert.x instances in different physical locations. CopyCat
provides a simple API for creating nodes as well as a number of other
tools which will be covered later.

For simple access to CopyCat APIs, create a `CopyCat` instance.

```java
public class MyVerticle extends Verticle {

  public void start() {
    CopyCat copycat = new CopyCat(this);
  }

}
```

From there, we can create a node by simply calling `createNode`

```java
CopyCatNode node = copycat.createNode("test.1");
```

We pass a unique address for the node as the only argument. This is the
address of the node being deployed. To start the node, simply call the
`start` method:
* `start()`
* `start(Handler<AsyncResult<Void>> doneHandler)`

But the node doesn't do much right now. Let's see how to register state
machine commands on the node.

#### Setting up cluster membership
In order for our node to know which other nodes exist in the cluster, we
have to tell it by updating its `ClusterConfig`

```java
node.getClusterConfig().addMember("test.2");
node.getClusterConfig().addMember("test.3");
```

This will tell the node to participate in leader elections and state
replications with `test.2` and `test.3`. Similarly, when the `test.2`
and `test.3` nodes are deployed, their configurations will need to contain
`test.1`. Note that *even after the node is deployed you can update the
cluster configuration*. When the cluster configuration is updated while
the node is running, the new configuration will be logged and replicated
to all the other *known* nodes in the cluster. This means that updating
the configuration on one node will result in the configuration being
updated on all nodes. The updated configuration will be persisted on all
nodes and thus will remain after failures as well.

#### Registering state machine commands
CopyCat state machines are represented as a series of commands that are
serviced by the cluster. Commands are registered by calling the
`registerCommand` method on a `CopyCatNode` instance.

```java
node.registerCommand("set", new Function<Command<JsonObject>, Boolean>() {
  public Boolean call(Command<JsonObject> command) {
    String key = command.data().getString("key");
    Object value = command.data().getValue("value");
    map.put(key, value);
    return true;
  }
});
```

Commands are simply functions that receive a command object containing
a specific argument type and return output. Commands don't necessarily
have to alter the state of the node. For instance, a `put` and `delete`
would alter the node's state, but `get` wouldn't because it is a *read-only*
action. This allows CopyCat to make some performance improvements by ignoring
read-only commands for replication purposes.

To mark a command as *read-only*, pass a `Command.Type` as the second
argument to `registerCommand`

```java
node.registerCommand("get", Command.Type.READ, new Function<Command<String>, Object>() {
  ...
});
```

### Submitting commands to the cluster
With commands registered on the node, we can start the node and begin
to submit commands.

```java
JsonObject command = new JsonObject()
    .putString("key", "foo")
    .putString("value", "Hello world!");
node.submitCommand("set", command, new Handler<AsyncResult<Boolean>>() {
  public void handle(AsyncResult<Boolean> result) {
    if (result.succeeded() && result.result()) {
      // Successfully set "foo" to "Hello world!"
    }
  }
});
```

Now, we can read the store key value back from the node.

```java
JsonObject command = new JsonObject()
    .putString("key", "foo");
node.submitCommand("get", command, new Handler<AsyncResult<String>>() {
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

#### Submitting commands over the event bus
What we've seen so far is a method for submitting commands to the cluster
via the `submitCommand` method. But this is Vert.x, and in Vert.x we expose
APIs over the event bus. CopyCat provides a `CopyCatServiceEndpoint` class for
exposing clusters over the event bus. To create an endpoint call the
`createEndpoint` method on a `CopyCat` instance, passing the cluster
address as the only argument.

```java
CopyCatServiceEndpoint endpoint = copycat.createEndpoint("test");
endpoint.start();
```

In the case of endpoints, all nodes should expose an endpoint *at the
same event bus address*. Vert.x event bus behavior will distribute requests
between each endpoint instance across the cluster.

To submit a command over the event bus, send a `JsonObject` message to
the endpoint address with a `command` and `data`.

```java
JsonObject command = new JsonObject()
    .putString("command", "get")
    .putObject("data", new JsonObject().putString("key", "foo"));
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

#### Automatic cluster detection
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
        Node node = copycat.createNode("test.1", config);
      }
    }
  });
```

Any time the cluster membership changes (a node dies), the local node
will be informed of the membership change and the new configuration will
be replicated across the cluster. Then, once the dead node comes back online,
the cluster configuration will again be updated and replicated, and
the rest of the cluster state will be replicated to the re-joining node.

### CopyCat Services
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
that will be exposed by the `CopyCatServiceEndpoint` on the Vert.x event bus.
Services operate much like nodes. Indeed, they expose the same API as
nodes aside from the `setBroadcastAddress(String address)` method, which
in most cases should not even be used since CopyCat will create a consistent
cluster broadcast address internally. So, to start the service we simply
register our commands and call the `start` method as with the `CopyCatNode`.

```java
service.registerCommand("set", new Function<Command<JsonObject>, Boolean>() {
  ...
});
service.registerCommand("get", Command.Type.READ, new Function<Command<JsonObject>, Object>() {
  ...
});
service.registerCommand("del", new Function<Command<JsonObject>, Boolean>() {
  ...
});
service.start();
```

CopyCat will handle cluster membership detection and configuration internally,
as well as expose the service address - in this case `test` - and all the
service commands over the Vert.x event bus. When multiple instances of the
service are started they form a fault-tolerant service.
