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
1. [Working with replicas](#working-with-replicas)
   * [Creating nodes](#creating-nodes)
   * [Configuring the cluster](#configuring-the-cluster)
   * [Submitting commands](#submitting-commands)
1. [Working with state machines](#working-with-state-machines)
   * [Creating state machines](#creating-state-machines)
   * [Creating state machine commands](#creating-state-machine-commands)
   * [Defining command arguments](#defining-command-arguments)
1. [Working with snapshots](#working-with-snapshots)
   * [Taking snapshots of the machine state](#taking-snapshots-of-the-state-machine)
   * [Taking snapshots with getters](#taking-snapshots-with-getters)
   * [Installing snapshots with setters](#installing-snapshots-with-setters)
   * [Taking snapshots of state machine fields](#taking-snapshots-of-state-machine-fields)
   * [Taking snapshots of fields with getters](#taking-snapshots-of-fields-with-getters)
   * [Installing snapshots of fields with setters](#installing-snapshots-of-fields-with-setters)
1. [Building a fault-tolerant in-memory key-value store](#a-simple-fault-tolerant-key-value-store)

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

## Working with replicas
Replicas are essentially single nodes within a CopyCat cluster. Each
replica is backed by a persistent log of commands that have been replicated
to the node. Also, replicas expose an interface to the CopyCat cluster,
allowing users to submit commands to the cluster.

### Creating nodes
Each replica represents a single node in the cluster. When a replica is
started, it will coordinate with other known nodes in the cluster to submit
commands and receive replicated log entries.

```java
public class MyVerticle extends Verticle {

  public void start() {
    Replica replica = new DefaultReplica("test", this);
    replica.start();
  }

}
```

### Configuring the cluster
In order for replicas to be able to coordinate command submissions with other
nodes in the cluster, they need to be notified of the cluster membership.
Cluster membership is configured using a `ClusterConfig` object which can
be accessed by the `Replica.config()` method. The `ClusterConfig` class
exposes the following methods:
* `addMember(String address)`
* `removeMember(String address)`
* `setMembers(String... addresses)`
* `setMembers(Set<String> addresses)`
* `getMembers()`

The cluster configuration should list the addresses of all the nodes in the
cluster. CopyCat also supports runtime cluster configuration changes. When
the cluster configuration is changed while the replica is running, the
updated configuration will be sent to the cluster leader just like a regular
command. The cluster leader will then log the configuration change and
replicate it to the rest of the cluster.

```java
Replica replica = new Replica("test", this, stateMachine);
replica.config().addMember("foo");
replica.config().addMember("bar");
replica.config().addMember("baz");
```

The configuration change process is actually a two-phase process that ensures
that consistency remains intact during the transition. When the leader receives
a configuration change, it actually replicates two separate log entries - one
with the combined old and new configuration, and - once that configuration
has been replicated and committed - the final updated configuration. This
ensures that split majorities cannot occur during the transition.

### Submitting commands
To submit a command to the cluster, use the `Replica.submitCommand` method.
When a command is submitted to a CopyCat cluster, the command is forwarded
to the cluster leader. Once the command has been replicated to the rest
of the cluster, it will be applied to the leader's state machine and the
result will be sent back to the requester.

```java
replica.submitCommand("get", new JsonObject().putString("key", "foo"), new Handler<AsyncResult<Object>>() {
  public void handle(AsyncResult<Object> result) {
    if (result.succeeded()) {
      Object value = result.result();
    }
  }
});
```

Each command call takes a `JsonObject` containing command arguments. Command
argument requirement depend on specific command implementations. More on
implementing commands in a moment.

## Working with state machines
A state machine is a model of a machine whose state changes over time.
Multiple instances of the same state machine will always arrive at the
same state given the same set of commands in the same order. This means
it is important that all state machines behave the same way. When copy cat
replicates application state, it's actually replicating a log of commands
which will be applied to each state machine once they've been reliably
replicated to the majority of the cluster.

### Creating state machines
State machines are defined by simply implementing the `StateMachine`
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

### Defining command arguments
Most of the time, commands require arguments to be passed when the user submits
a command to the cluster. Command arguments are submitted in the form of a
`JsonObject` instance, but CopyCat annotations can be used to parse and validate
the json arguments before executing the command.

To define a command argument, use the `@Command.Argument` parameter annotation.
This annotation accept a `value` which is the argument field name and optionally
a `required` boolean, which defaults to `true`.

```java
@Command(name="get", type=Command.Type.READ)
public String get(@Argument("key") String key) {
  return data.get(key);
}
```

You can also use the `@Command.Arguments` method annotation to define all
arguments in the order in which they appear.

```java
@Command(name="get", type=Command.Type.READ)
@Arguments({
  @Argument(name="key"),
  @Argument(name="value")
})
public String put(String key, Object value) {
  return data.put(key, value);
}
```

## Working with snapshots
CopyCat provides a dynamic API for persisting the system state. When logs
begin to grow too large, CopyCat will automatically take a snapshot of the
state machine state, write it to disk, and flush the logs. If the node
fails, once the node is restarted CopyCat will load the perisisted state,
apply it to the state machine, and continue normal operation.

### Taking snapshots of the machine state
The simplest method of supporting snapshotting in CopyCat is to apply the
`@StateValue` to any field within the state machine.

```java
public class MyStateMachine implements StateMachine {

  @StateValue
  private final Map<String, Object> data = new HashMap<>();

}
```

When performing log compaction, CopyCat will serialize a single `@StateValue`
annotated field. Note that the field value must be serializable by
[Jackson](http://jackson.codehaus.org/). If you need to provide custom
serialization for Jackson, I recommend you use
[Jackson Annotations](http://fasterxml.github.io/jackson-annotations/javadoc/2.2.0/).

### Taking snapshots with getters
If you need to perform some processing of the state machine state prior
to CopyCat serializing and persisting it, you can provide a state getter
with the `@StateGetter` annotation. This can be used to convert state
into a `JsonObject` or `JsonArray` instance if necessary.

```java
public class MyStateMachine implements StateMachine {
  private Map<String, Object> data = new HashMap<>();

  @StateGetter
  public Map<String, Object> takeSnapshot() {
    return data;
  }

}
```

### Installing snapshots with setters
If you've defined a `@StateGetter` for a state machine, you may also want
to provide a `@StateSetter` method for when CopyCat installs the state
at startup (after failures). State setters should be one argument and
expect whatever serializable value type was provided by the associated
state field or getter.

```java
public class MyStateMachine implements StateMachine {

  private Map<String, Object> data = new HashMap<>();

  @StateSetter
  public void installSnapshot(Map<String, Object> data) {
    this.data = data;
  }

}
```

### Taking snapshots of state machine fields
CopyCat provides annotations for building snapshots from multiple fields
and methods. When field-level snapshots are constructed, each field becomes
a property of a json object which combines fields into a complete state.

The simplest method of snapshotting multiple state machine fields is by
using the `@StateField` annotation on an instance field.

```java
public class MyStateMachine implements StateMachine {

  @StateField
  private String name;

  @StateField
  private Map<String, Object> data = new HashMap<>();

}
```

If no `value` is provided for the annotation, the state property will
be the name of the field. In most cases naming properties is not necessary
since Java syntax already prevents name collisions.

### Taking snapshots of fields with getters
Like complete state snapshots, individual fields can also be snapshotted
using getters. To define a field-level getter, use the `@StateFieldGetter`
annotation.

```java
public class MyStateMachine implements StateMachine {

  @StateField
  private String name;

  private Map<String, Object> data = new HashMap<>();

  @StateFieldGetter("data")
  public Map<String, Object> getData() {
    return data;
  }

}
```

The `@StateFieldGetter` requires a `value` that indicates the property to
which the serialized value will be assigned in the complete state. This
should be unique to the state, and is used to associate getters with setters.

### Installing snapshots of fields with setters
Of course, you can install field-level snapshots in the same (opposite)
manner with the `@StateFieldSetter` annotation. Again, the field setter
annotation requires a `value` that indicates the property name. The
setter `value` should match the getter `value` for the getter for the
same state field.

```java
public class MyStateMachine implements StateMachine {

  @StateField
  private String name;

  private Map<String, Object> data = new HashMap<>();

  @StateFieldSetter("data")
  public void setData(Map<String, Object> data) {
    this.data = data;
  }

}
```

## A Simple Fault-Tolerant Key-Value Store
To demonstrate the tools that CopyCat provides, this is a simple example
of a Redis-style fault-tolerant in-memory key-value store exposed over
the Vert.x event bus.

```java
public class KeyValueStore extends Verticle implements StateMachine {

  @StateValue
  private final Map<String, Object> data = new HashMap<>();

  @Command(name="get", type=Command.Type.READ)
  public Object get(@Argument("key") String key, @Argument(value="default", required=false) Object defaultValue) {
    return data.containsKey(key) ? data.get(key) : defaultValue;
  }

  @Command(name="set", type=Command.Type.WRITE)
  public boolean set(@Argument("key") String key, @Argument("value") Object value) {
    data.put(key, value);
    return true;
  }

  @Command(name="del", type=Command.Type.WRITE)
  public boolean del(@Argument("key") String key) {
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
    final Replica replica = new DefaultReplica(address, this, this);

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
