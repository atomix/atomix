CopyCat
=======

CopyCat is a fault-tolorant replication framework for Vert.x. It provides
for state machine replication using the Raft consensus algorithm as described in
[this paper](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

**This project is still very much under development and is recommended for testing only**

This project is being developed as the potential basis of future development of
[Vertigo](http://github.com/kuujo/vertigo) - improved cluster and state management.
This project is also a long way from stability, but I'm making it public in the hopes
of it gaining interest, contributions, and particularly reviews from those who are
knowledgable about the Raft consensus algorithm. Please feel free to poke, prod,
and submit changes as necessary.

## Table of contents
1. [Features](#features)
1. [How it works](#how-it-works)

### Features
* Automatically replicates state across multiple Vert.x instances in a consistent and reliable manner
* Supports runtime cluster membership changes
* Supports replicated log persistence in MongoDB, Redis, or in memory
* Supports log compaction using snapshots
* Uses adaptive failure detection for fast leader re-election

## How it works
CopyCat is a Java-specific library that provides tools for creating fault-tolerant
Vert.x applications by replicating state across multiple Vert.x instances and
coordinating requests to the service. When multiple CopyCat replicas are started
within a Vert.x cluster, the replicas communicate with each other to elect a
leader which coordinates the cluster and replicates commands to all the nodes
in the cluster. CopyCat's leader election and replication is performed using
a modified implementation of the [Raft](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf)
consensus algorithm. The replication of CopyCat cluster states means that
if any member of the cluster dies, the state is not lost. The cluster will
simply coordinate to share state, and once the dead replica re-joins the
cluster, any commands missed while the node was down will be replicated
to the node.

## Creating a state machine
A state machine is a model of a machine whose state changes over time.
Multiple instances of the same state machine will always arrive at the
same state given the same set of commands in the same order. This means
it is important that all state machines behave the same way. When copy cat
replicates application state, it's actually replicating a log of commands
which will be applied to each state machine once they've been reliably
replicated to the majority of the cluster.

CopyCat state machines are defined by implementing the `StateMachine`
interface.

```java
public class MyStateMachine implements StateMachine {

}
```

The `StateMachine` interface is simply an identifier interface as it does
not expose any methods on its own. Instead, the interface is created by
annotating the `StateMachine` implementation.

### Creating state machine commands
CopyCat state machines are made up of any number of commands. Commands are
named methods that receive input and provide output. Remember, state machine
commands should always provide the same output given the same arguments and
state.

To create a state machine command, use the `@Command` annotation. Each command
should be given a `name` which is used to identify the command method when
users submit commands to the CopyCat cluster.

```java
@Command(name="get")
public String get() {
  return "Hello world!";
}
```

The `@Command` annotation also has an additional `type` parameter. The type
is a `Command.Type` which indicates how the command should be logged and
replicated within the CopyCat cluster. For instance, since read-only commands
do not impact the state of the state machine, they do not need to be logged
and replicated. Specifying the `type` command can help improve read performance
significantly.

```java
@Command(name="get", type=Command.Type.READ)
public String get() {
  return "Hello world!";
}
```

There are three types of `Command.Type`:
* `Command.Type.READ` - indicates a read-only command
* `Command.Type.WRITE` - indicates a write-only command (state altering)
* `Command.Type.READ_WRITE` - indicates a read/write command. This is the default

### Extracting command arguments
Most of the time, commands require arguments to be passed when the user submits
a command to the cluster. Command arguments are submitted in the form of a
`JsonObject` instance, but CopyCat annotations can be used to parse and validate
the json arguments before executing the command.

To define a command argument, use the `@Command.Argument` parameter annotation.
This annotation accept a `value` which is the argument field name and optionally
a `required` boolean, which defaults to `true`.

```java
@Command(name="get", type=Command.Type.READ)
public String get(@Command.Argument("key") String key) {
  return data.get(key);
}
```

You can also use the `@Command.Arguments` method annotation to define all
arguments in the order in which they appear.

```java
@Command(name="get", type=Command.Type.READ)
@Command.Arguments({
  @Command.Argument(name="key"),
  @Command.Argument(name="value")
})
public String put(String key, Object value) {
  return data.put(key, value);
}
```

## Creating snapshots
CopyCat provides a couple of methods of providing and installing snapshot. The
simplest way to create snapshots is by annotating a state machine field with the
`@Snapshot` annotation. CopyCat will take a snapshot of the field's value when
the logs grow too big. This is ideal for state machines that simply store data
in a `Map` or `List`.

```java
public class MyStateMachine implements StateMachine {

  @Snapshot
  private final Map<String, Object> data = new HashMap<>();

}
```

The snapshot field can be any type that is serializable by Jackson.

Alternatively, users can provide methods for providing and installing snapshots.

To create a snapshot provider, use the `@Snapshot.Provider` annotation.

```java
public class MyStateMachine implements StateMachine {
  private Map<String, Object> data = new HashMap<>();

  @Snapshot.Provider
  public Map<String, Object> takeSnapshot() {
    return data;
  }

}
```

The snapshot provider can return any type that can be serialized
by Jackson. This includes maps and collections.

To create a snapshot installer, use the `@Snapshot.Installer` annotation.

```java
public class MyStateMachine implements StateMachine {
  private Map<String, Object> data = new HashMap<>();

  @Snapshot.Installer
  public void installSnapshot(Map<String, Object> data) {
    this.data = data;
  }

}
```

The installer can take whatever value was returned by the provider as an
argument.

## A Simple Key-Value Store
To demonstrate the tools that CopyCat provides, this is a simply example
of a Redis-style fault-tolerant in-memory key-value store exposed over
the Vert.x event bus.

```java
public class KeyValueStore extends Verticle implements StateMachine {
  private final Map<String, Object> data = new HashMap<>();

  @Command(name="get", type=Command.Type.READ)
  public Object get(@Command.Argument("key") String key, @Command.Argument(value="default", required=false) Object defaultValue) {
    return data.containsKey(key) ? data.get(key) : defaultValue;
  }

  @Command(name="set", type=Command.Type.WRITE)
  public boolean set(@Command.Argument("key") String key, @Command.Argument("value") Object value) {
    data.put(key, value);
    return true;
  }

  @Command(name="del", type=Command.Type.WRITE)
  public boolean del(@Command.Argument("key") String key) {
    if (data.containsKey(key)) {
      data.remove(key);
      return true;
    }
    return false;
  }

  @Override
  public void start(final Future<Void> startResult) {
    String address = container.config().getString("address");
    JsonArray cluster = container.config().getArray("cluster");

    // Create a replica.
    final Replica replica = new DefaultReplica(address, vertx, this);

    // Add members to the cluster so we will share state with them.
    for (Object member : cluster) {
      replica.config().addMember((String) member);
    }

    // Start the replica.
    replica.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          startResult.setFailure(result.cause());
        }
        else {
          // Register a handler to allow commands to be submitted to the cluster via the event bus.
          vertx.eventBus().registerHandler("keyvalue", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {

              // Submit the command.
              replica.submitCommand(message.body().getString("command"), message.body(), new Handler<AsyncResult<Object>>() {
                public void handle(AsyncResult<Object> result) {
                  if (result.failed()) {
                    message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
                  }
                  else {
                    message.reply(new JsonObject().putString("status", "ok").putValue("result", result.result()));
                  }
                }
              });
            }
          });
        }
      }
    });
  }

}
```

This example demonstrates a fault-tolerant key-value store. To start the
store, all we need to do is simply run the verticle in cluster mode, passing
an `address` and an array of `cluster` members as the verticle configuration.

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
vertx.eventBus().send("keyvalue", command, new Handler<Message<JsonObject>>() {
  public void handle(Message<JsonObject> message) {
    if (message.body().getString("status").equals("ok")) {

      // Create a get command
      JsonObject command = new JsonObject()
        .putString("command", "get")
        .putString("key", "foo");

      // Get the "foo" key
      vertx.eventBus().send("keyvalue", command, new Handler<Message<JsonObject>>() {
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
