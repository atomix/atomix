CopyCat
=======

### [User Manual](#user-manual) | [Tutorials](#tutorials)

CopyCat is an extensible Java-based implementation of the
[Raft consensus protocol](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

The core of CopyCat is a framework designed to support a variety of protocols and
transports. CopyCat provides a simple extensible API that can be used to build a
strongly consistent, fault/partition tolerant state machine over any mode of communication.
CopyCat's Raft implementation also supports advanced features of the Raft algorithm such as
snapshotting and dynamic cluster configuration changes and provides additional optimizations
for various scenarios such as failure detection and read-only state queries.

CopyCat is a pluggable framework, providing protocol and endpoint implementations for
various frameworks such as [Netty](http://netty.io) and [Vert.x](http://vertx.io).

User Manual
===========

1. [A Brief Introduction](#a-brief-introduction)
1. [How it works](#how-it-works)
   * [State machines](#state-machines)
   * [Commands](#commands)
   * [Logs](#logs)
   * [Snapshots](#snapshots)
   * [Cluster configurations](#cluster-configurations)
   * [Leader election](#leader-election)
      * [Followers](#followers)
      * [Candidates](#candidates)
      * [Leaders](#leaders)
   * [Protocols](#protocols)
   * [Endpoints](#endpoints)
1. [Getting started](#getting-started)
   * [Creating a state machine](#creating-a-state-machine)
   * [Providing command types](#providing-command-types)
   * [Taking snapshots](#taking-snapshots)
   * [Creating an annotated state machine](#creating-an-annotated-state-machine)
   * [Configuring the replica](#configuring-the-replica)
   * [Configuring the cluster](#configuring-the-cluster)
   * [Creating a dynamic cluster](#creating-a-dynamic-cluster)
   * [Creating the CopyCatContext](#creating-the-copycatcontext)
   * [Setting the log type](#setting-the-log-type)
   * [Submitting commands to the cluster](#submitting-commands-to-the-cluster)
1. [Serialization](#serialization)
   * [Providing custom log serializers](#providing-custom-log-serializers)
1. [Protocols](#protocols-1)
   * [Writing a protocol server](#writing-a-protocol-server)
   * [Writing a protocol client](#writing-a-protocol-client)
   * [Injecting URI arguments into a protocol](#injecting-uri-arguments-into-a-protocol)
   * [Using multiple URI annotations on a single parameter](#using-multiple-uri-annotations-on-a-single-parameter)
   * [Making annotated URI parameters optional](#making-annotated-uri-parameters-optional)
   * [Injecting URI arguments via annotated setters](#injecting-uri-arguments-annotated-setters)
   * [The complete protocol](#the-complete-protocol)
   * [Built-in protocols](#built-in-protocols)
      * [Direct](#direct-protocol)
      * [Netty TCP](#netty-tcp-protocol)
      * [Vert.x Event Bus](#vertx-event-bus-protocol)
      * [Vert.x TCP](#vertx-tcp-protocol)
1. [Endpoints](#endpoints-1)
   * [Using URI annotations with endpoints](#using-uri-annotations-with-endpoints)
   * [Wrapping the CopyCatContext in an endpoint service](#wrapping-the-copycatcontext-in-an-endpoint-service)
   * [Built-in endpoints](#built-in-endpoints)
      * [Vert.x Event Bus](#vertx-event-bus-endpoint)
      * [Vert.x TCP](#vertx-tcp-endpoint)
      * [Vert.x HTTP](#vertx-http-endpoint)

# A brief introduction
CopyCat is a "protocol agnostic" implementation of the Raft consensus algorithm. It
provides a framework for constructing a partition-tolerant replicated state machine
over a variety of wire-level protocols. It sounds complicated, but the API is actually
quite simple. Here's a quick example.

```java
public class KeyValueStore extends AnnotatedStateMachine {
  @Stateful
  private Map<String, Object> data = new HashMap<>();

  @Command(name = "get", type = Command.Type.READ)
  public Object get(@Argument("key") String key) {
    return data.get(key);
  }

  @Command(name = "set", type = Command.Type.WRITE)
  public void set(@Argument("key") String key, @Argument("value") Object value) {
    data.put(key, value);
  }

  @Command(name = "delete", type = Command.Type.WRITE)
  public void delete(@Argument("key") String key) {
    data.remove(key);
  }

}
```

Here we've created a simple key value store. By deploying this state machine on several
nodes in a cluster, CopyCat will ensure commands (i.e. `get`, `set`, and `delete`) are
applied to the state machine in the order in which they're submitted to the cluster
(log order). Internally, CopyCat uses a replicated log to order and replicate commands,
and it uses leader election to coordinate log replication. When the replicated log grows
too large, CopyCat will take a snapshot of the `@Stateful` state machine state and
compact the log.

```java
Log log = new FileLog("key-value.log");
```

To configure the CopyCat cluster, we simply create a `ClusterConfig`.

```java
ClusterConfig cluster = new ClusterConfig();
cluster.setLocalMember("tcp://localhost:8080");
cluster.setRemoteMembers("tcp://localhost:8081", "tcp://localhost:8082");
```

Note that the cluster configuration identifies a particular protocol, `tcp`. These are
the endpoints the nodes within the CopyCat cluster use to communicate with one another.
CopyCat provides a number of different [protocol](#protocols) and [endpoint](#endpoints)
implementations.

Additionally, CopyCat cluster membership is dynamic, and the `ClusterConfig` is `Observable`.
This means that if the `ClusterConfig` is changed while the cluster is running, CopyCat
will pick up the membership change and replicate the cluster configuration in a safe manner.

Now that the cluster has been set up, we simply create a `CopyCat` instance, specifying
an [endpoint](#endpoints) through which the outside world can communicate with the cluster.

```java
CopyCat copycat = new CopyCat("http://localhost:5000", new KeyValueStore(), log, cluster);
copycat.start();
```

That's it! We've just created a strongly consistent, fault-tolerant key-value store with an
HTTP API in less than 25 lines of code!

```java
public class StronglyConsistentFaultTolerantAndTotallyAwesomeKeyValueStore extends AnnotatedStateMachine {

  public static void main(String[] args) {
    // Create the local file log.
    Log log = new FileLog("key-value.log");

    // Configure the cluster.
    ClusterConfig cluster = new ClusterConfig();
    cluster.setLocalMember("tcp://localhost:8080");
    cluster.setRemoteMembers("tcp://localhost:8081", "tcp://localhost:8082");

    // Create and start a server at localhost:5000.
    new CopyCat("http://localhost:5000", new StronglyConsistentFaultTolerantAndTotallyAwesomeKeyValueStore(), log, cluster).start();
  }

  @Stateful
  private Map<String, Object> data = new HashMap<>();

  @Command(name = "get", type = Command.Type.READ)
  public Object get(@Argument("key") String key) {
    return data.get(key);
  }

  @Command(name = "set", type = Command.Type.WRITE)
  public void set(@Argument("key") String key, @Argument("value") Object value) {
    data.put(key, value);
  }

  @Command(name = "delete", type = Command.Type.WRITE)
  public void delete(@Argument("key") String key) {
    data.remove(key);
  }

}
```

We can now execute commands on the state machine by making `POST` requests to
the HTTP interface.

```
POST http://localhost:5000/set
{
  "key": "foo",
  "value": "Hello world!"
}

200 OK

POST http://localhost:5000/get
{
  "key": "foo"
}

200 OK

{
  "result": "Hello world!"
}

POST http://localhost:5000/delete
{
  "key": "foo"
}

200 OK
```

# How it works
CopyCat uses a Raft-based consensus algorithm to perform leader election and state
replication. Each node in a CopyCat cluster may be in one of three states at any
given time - [follower](#followers), [candidate](#candidates), or [leader](#leaders).
Each node in the cluster maintains an internal [log](#logs) of replicated commands.
When a command is submitted to a CopyCat cluster, the command is forwarded to the
cluster leader. The leader then logs the command and replicates it to a majority
of the cluster. Once the command has been replicated, it applies the command to its
local state machine and replies with the command result.

*For the most in depth description of how CopyCat works, see
[In Search of an Understandable Consensus Algorithm](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf)
by Diego Ongaro and John Ousterhout.*

### State Machines
Each node in a CopyCat cluster contains a state machine to which the node applies
[commands](#commands) sent to the cluster. State machines are simply classes that
implement CopyCat's `StateMachine` interface, but there are a couple of very important
aspects to note about state machines.
* Given the same commands in the same order, state machines should always arrive at
  the same state with the same output.
* Following from that rule, each node's state machine should be identical.

### Commands
Commands are state change instructions that are submitted to the CopyCat cluster
and ultimately applied to the state machine. For instance, in a key-value store,
a command could be something like `get` or `set`. CopyCat provides unique features
that allow it to optimize handling of certain command types. For instance, read-only
commands which don't contribute to the system state will not be replicated, but commands
that do alter the system state are eventually replicated to all the nodes in the cluster.

### Logs
CopyCat nodes share state using a replicated log. When a command is submitted to the
CopyCat cluster, the command is appended to the [leader's](#leader-election) log and
replicated to other nodes in the cluster. If a node dies and later restarts, the node
will rebuild its internal state by reapplying logged commands to its state machine.
The log is essential to the operation of the CopyCat cluster in that it both helps
maintain consistency of state across nodes and assists in [leader election](#leader-election).
CopyCat provides both in-memory logs for testing and file-based logs for production.

### Snapshots
In order to ensure [logs](#logs) do not grow too large for the disk, CopyCat replicas periodically
take and persist snapshots of the [state machine](#state-machines) state. CopyCat manages snapshots
using a method different than is described in the original Raft paper. Rather than persisting
snapshots to separate snapshot files and replicating snapshots using an additional RCP method,
CopyCat appends snapshots directly to the log. This allows CopyCat to minimize complexity by
transfering snapshots as a normal part of log replication.

Normally, when a node crashes and recovers, it restores its state using the latest snapshot
and then rebuilds the rest of its state by reapplying committed [command](#commands) entries.
But in some cases, replicas can become so far out of sync that the cluster leader has already
written a new snapshot to its log. In that case, the [leader](#leader-election) will replicate
its latest snapshot to the recovering node, allowing it to start with the leader's snapshot.

### Cluster Configurations
CopyCat replicas communicate with one another through a user-defined cluster configuration.
The CopyCat cluster is very flexible, and the [protocol](#protocols) underlying the CopyCat
cluster is pluggable. Additionally, for cases where cluster configuration may change over
time, CopyCat supports runtime cluster configuration changes. As with other state changes,
all cluster configuration changes are performed through the cluster leader.

### Leader Election
CopyCat clusters use leader election to maintain synchronization of nodes. In CopyCat,
all state changes are performed via the leader. In other words, commands that are submitted
to the cluster are *always* forwarded to the leader for processing. This leads to a much
simpler implementation of single-copy consistency.

Raft's leader election algorithm relies largely on the log to elect a leader. Each node
in the cluster can serve one of three roles at any given time - [follower](#followers),
[candidate](#candidates), and [leader](#leaders) - each of which serve a specific role
in contributing to elections, replicating logs, and maintaining general consistency
of state across the cluster.

### Followers
When a node is first started, it is initialized as a follower. The follower
serves only to listen for synchronization requests from cluster leaders and apply
replicated log entries to its [state machine](#state-machines). If the follower does not
receive a request from the cluster leader for a configurable amount of time, it will
transition to a [candidate](#candidates) and start a new election.

### Candidates
The candidate role occurs when a replica is announcing its candidacy to become the
cluster leader. Candidacy occurs when a [follower](#followers) has not heard from the
cluster leader for a configurable amount of time. When a replica becomes a candidate, it
requests votes from each other member of the cluster. The voting algorithm is essential
to the consistency of CopyCat logs. When a replica receives a vote request, it compares
the status of the requesting node's log to its own log and decides whether to vote
for the candidate based on how up-to-date the candidates log is. This ensures that
only replicas with the most up-to-date logs can become the cluster leader.

### Leaders
Once a replica has won an election, it transitions to the leader role. The leader
is the node through which all commands and configuration changes are performed.
When a command is submitted to the cluster, the command is forwarded to the leader.
Based on the command type - read or write - the leader will then replicate the command
to its [followers](#followers), apply it to its [state machine](#state-machines), and return
the result. The leader is also responsible for maintaining log consistency during
[cluster configuration changes](#cluster-configurations).

### Protocols
The `copycat-core` project is purely an implementation of the Raft consensus algorithm.
It does not implement any specific transport aside from a `direct` transport for testing.
Instead, the CopyCat API is designed to allow users to implement the transport layer
using the `Protocol` API. CopyCat does, however, provide some core protocol implementations
in the `copycat-netty` and `copycat-vertx` projects.

### Endpoints
CopyCat provides a framework for building fault-tolerant [state machines](#state-machines) on
the Raft consensus algorithm, but fault-tolerant distributed systems are of no use if they can't
be accessed from the outside world. CopyCat's `Endpoint` API facilitates creating user-facing
interfaces (servers) for submitting [commands](#commands) to the CopyCat cluster.

# Getting Started

### Creating a [state machine](#state-machines)
To create a state machine in CopyCat, simply implement the `StateMachine`
interface. The state machine interface exposes three methods:

```java
public interface StateMachine {

  Map<String, Object> takeSnapshot();

  void installSnapshot(Map<String, Object> snapshot);

  Object applyCommand(String command, Map<String, Object> args);

}
```

The first two methods are for snapshot support, but more on that later. The most
important method in the state machine is the `applyCommand` method. What's important
to remember when writing a state machine is: the machine should always arrive at
the same state and provide the same output given the same commands in the same order.
This means your state machine should not rely on mutable data sources such as databases.

### Providing [command types](#commands)
When a command is submitted to the CopyCat cluster, the command is first written
to a [log](#logs) and replicated to other nodes in the cluster. Once the log entry has
been replicated to a majority of the cluster, the command is applied to the leader's
state machine and the response is returned.

In many cases, CopyCat can avoid writing any data to the log at all. In particular,
when a read-only command is submitted to the cluster, CopyCat does not need to log
and replicate that command since it has no effect on the machine state. Instead, CopyCat
can simply ensure that the cluster is in sync, apply the command to the state machine,
and return the result. However, in order for it to do this it needs to have some
additional information about each specific command.

To provide command information to CopyCat, implement the `CommandProvider` interface.
When a command is submitted to the cluster, CopyCat will check whether the state machine
is a `CommandProvider`, and if so, use command info to determine how to handle the command.

Each command can have one of three types:
* `READ`
* `WRITE`
* `READ_WRITE`

By default, all commands are of the type `READ_WRITE`. While there is no real difference
between the `WRITE` and `READ_WRITE` types, the `READ` type is important in allowing
CopyCat to identify read-only commands. *It is important that any command identified as
a `READ` command never modify the machine state.*

Let's look at an example of a command provider:

```java
public class MyStateMachine implements StateMachine, CommandProvider {
  private static final Command READ = new GenericCommand("read", Command.Type.READ);
  private static final Command WRITE = new GenericCommand("write", Command.Type.WRITE);
  private static final Command NONE = new GenericCommand("none", Command.Type.READ_WRITE);

  @Override
  public Command getCommand(String command) {
    switch (command) {
      case "read":
        return READ;
      case "write":
        return WRITE;
      default:
        return NONE;
    }
  }

}
```

Note that `Command` is actually an annotation, so CopyCat provides a helper class for
constructing annotations call `GenericCommand`.

### Taking [snapshots](#snapshots)
One of the issues with a [replicated log](#logs) is that over time it will only continue to grow.
There are a couple of ways to potentially handle this, and CopyCat uses the [snapshotting](#snapshots)
method that is recommended by the authors of the Raft algorithm. However, in favor of simplicity,
the CopyCat snapshotting implementation does slightly differ from the one described in the Raft
paper. Rather than storing snapshots in a separate snapshot file, CopyCat stores snapshots as
normal log entries, making it easier to replicate snapshots when replicas fall too far out
of sync. Additionally, CopyCat guarantees that the first entry in any log will always be
a `SnapshotEntry`, again helping to ease the process of replicating snapshots to far
out-of-date replicas.

All snapshot serialization, storage, and loading is handled by CopyCat internally. Users
need only create and install the data via the `takeSnapshot` and `installSnapshot` methods
respectively. Once the log grows to a predetermined size (configurable in `CopyCatConfig`),
CopyCat will take a snaphsot of the log and wipe all previous log entries.

Snapshots are simply `Map<String, Object>` maps.

```java
public class MyStateMachine implements StateMachine {
  private Map<String, Object> data = new HashMap<>();

  @Override
  public Map<String, Object> takeSnapshot() {
    return data;
  }

  @Override
  public void installSnapshot(Map<String, Object> data) {
    this.data = data;
  }

  @Override
  public Object applyCommand(String command, Map<String, Object> args) {
    switch (command) {
      case "get":
        return data.get(args.get("key"));
      case "set":
        args.put(args.get("key"), args.get("value"));
        return null;
      case "delete":
        return args.remove(args.get("key"));
      default:
        throw new UnsupportedOperationException();
    }
  }

}
```

### Creating an annotated [state machine](#state-machines)
CopyCat provides a helpful base class which supports purely annotation based state
machines. To create an annotated state machine, simply extend the `AnnotatedStateMachine`
class. This is the recommended method for creating Java-based state machines.

```java
public class MyStateMachine extends AnnotatedStateMachine {
}
```

The annotated state machine will introspect itself to find commands defined with the
`@Command` annotation.

```java
public class MyStateMachine extends AnnotatedStateMachine {
  @Stateful
  private Map<String, Object> data = new HashMap<>();

  @Command(name = "get", type = Command.Type.READ)
  public Object get(@Argument("key") String key) {
    return data.get(key);
  }

  @Command(name = "set", type = Command.Type.WRITE)
  public void set(@Argument("key") String key, @Argument("value") Object value) {
    data.put(key, value);
  }

  @Command(name = "delete", type = Command.Type.READ_WRITE)
  public Object delete(@Argument("key") String key) {
    return data.remove(key);
  }

}
```

The `AnnotatedStateMachine` is a `CommandProvider` which handles locating and
providing command information based on annotations on the implementation. This
removed the need for mapping command names to `Command` instances for command
providers and `switch` statements for command application.

Note also that the `@Stateful` annotation is used to indicate that the `data` field
should be persisted whenever a snapshot is taken.

### Configuring the replica
CopyCat exposes a configuration API that allows users to configure how CopyCat behaves
during elections and how it replicates commands. To configure a CopyCat replica, create
a `CopyCatConfig` to pass to the `CopyCatContext` constructor.

```java
CopyCatConfig config = new CopyCatConfig();
```

The `CopyCatConfig` exposes the following configuration methods:
* `setElectionTimeout`/`withElectionTimeout` - sets the timeout within which a [follower](#follower)
must receive an `AppendEntries` request from the leader before starting a new election. This timeout
is also used to calculate the election timeout during elections. Defaults to `2000` milliseconds
* `setHeartbeatInterval`/`withHeartbeatInterval` - sets the interval at which the leader will send
heartbeats (`AppendEntries` requests) to followers. Defaults to `500` milliseconds
* `setRequireWriteQuorum`/`withRequireWriteQuorum` - sets whether to require a quorum during write
operations. It is strongly recommended that this remain enabled for consistency. Defaults to `true` (enabled)
* `setRequireReadQuorum`/`withRequireReadQuorum` - sets whether to require a quorum during read operations.
Read quorums can optionally be disabled in order to improve performance at the risk of reading stale data.
When read quorums are disabled, the leader will immediately respond to `READ` type commands by applying
the command to its state machine and returning the result. Defaults to `true` (enabled)
* `setMaxLogSize`/`withMaxLogSize` - sets the maximum log size before a snapshot should be taken. As
entries are appended to the local log, replicas will monitor the size of the local log to determine
whether a snapshot should be taken based on this value. Defaults to `32 * 1024^2`
* `setCorrelationStrategy`/`withCorrelationStrategy` - CopyCat generates correlation identifiers
for each request/response pair that it sends. This helps assist in correlating messages across
various protocols. By default, CopyCat uses `UuidCorrelationStrategy` - a `UUID` based correlation ID
generator, but depending on the protocol being used, the `MonotonicCorrelationStrategy` which generates
monotonically increasing IDs may be safe to use (it's safe with all core CopyCat protocols).
Defaults to `UuidCorrelationStrategy`
* `setTimerStrategy`/`withTimerStrategy` - sets the replica timer strategy. This allows users to
control how CopyCat's internal timers work. By default, replicas use a `ThreadTimerStrategy`
which uses the Java `Timer` to schedule delays on a background thread. Users can implement their
own `TimerStrategy` in order to, for example, implement an event loop based timer. Timers should
return monotonically increasing timer IDs that never repeat. Note also that timers do not need
to be multi-threaded since CopyCat only sets a timeout a couple time a second. Defaults to
`ThreadTimerStrategy`

### Configuring the [cluster](#cluster-configurations)
When a CopyCat cluster is first started, the [cluster configuration](#cluster-configurations)
must be explicitly provided by the user. However, as the cluster runs, explicit cluster
configurations may no longer be required. This is because once a cluster leader has been
elected, the leader will replicate its cluster configuration to the rest of the cluster,
and the user-defined configuration will be replaced by an internal configuration.

To configure the CopyCat cluster, create a `ClusterConfig`.

```java
ClusterConfig cluster = new ClusterConfig();
```

Each cluster configuration must contain a *local* member and a set of *remote* members.
To define the *local* member, pass the member address to the cluster configuration
constructor or call the `setLocalMember` method.

```java
cluster.setLocalMember("tcp://localhost:1234");
```

Note that the configuration uses URIs to identify nodes.
[The protocol used by CopyCat is pluggable](#protocols), so member address formats may
differ depending on the protocol you're using.

To set remote cluster members, use the `setRemoteMembers` or `addRemoteMember` method:

```java
cluster.addRemoteMember("tcp://localhost:1235");
cluster.addRemoteMember("tcp://localhost1236");
```

### Creating a [dynamic cluster](#cluster-configurations)
The CopyCat cluster configuration is `Observable`, and once the local node is elected
leader, it will begin observing the configuration for changes.

Once the local node has been started, simply adding or removing nodes from the observable
`DynamicClusterConfig` may cause the replica's configuration to be updated. However,
it's important to remember that as with commands, configuration changes must go through
the cluster leader and be replicated to the rest of the cluster. This allows CopyCat
to ensure that logs remain consistent while nodes are added or removed, but it also
means that cluster configuration changes may not be propagated for some period of
time after the `ClusterConfig` is updated.

### Creating the CopyCatContext
CopyCat replicas are run via a `CopyCatContext`. The CopyCat context is a container
for the replica's `Log`, `StateMachine`, and `ClusterConfig`. To start the replica,
simply call the `start()` method.

```java
CopyCatContext context = new CopyCatContext(stateMachine, cluster);
context.start();
```

### Setting the [log](#logs) type
By default, CopyCat uses an in-memory log for simplicity. However, in production
users should use a disk-based log. CopyCat provides two `Log` implementations:
* `MemoryLog` - an in-memory `TreeMap` based log
* `FileLog` - a file-based log

To set the log to be used by a CopyCat replica, simply pass the log instance in
the `CopyCatContext` constructor:

```java
CopyCatContext context = new CopyCatContext(new MyStateMachine(), new FileLog("my.log"), cluster);
context.start();
```

### Submitting [commands](#commands) to the cluster
To submit commands to the CopyCat cluster, simply call the `submitCommand` method
on any `CopyCatContext`.

```java
Map<String, Object> args = new HashMap<>();
args.put("key", "foo");
context.submitCommand("read", args, result -> {
  if (result.succeeded()) {
    Object value = result.value();
  }
});
```

When a command is submitted to a `CopyCatContext`, the command will automatically
be forwarded on to the current cluster [leader](#leaders). If the cluster does not have any
currently [elected](#leader-election) leader (or the node to which the command is submitted
doesn't know of any cluster leader) then the submission will fail.

# Serialization
CopyCat provides a pluggable serialization API that allows users to control how log
entries are serialized to the log. CopyCat provides two serializer implementations, one
using the core Java serialization mechanism, and one [Jackson](http://jackson.codehaus.org/)
based serializer (the default). To configure the serializer in use, add a file to the classpath
at `META-INF/services/net/kuujo/copycat/Serializer` containing the serializer factory class name:
* `net.kuujo.copycat.setializer.impl.JacksonSerializerFactory` (default)
* `net.kuujo.copycat.serializer.impl.JavaSerializerFactory`

### Providing custom log serializers
CopyCat locates log serializers via its custom service loader implementation. To provide
a custom serializer to CopyCat, simply add a configuration file to your classpath at
`META-INF/services/net/kuujo/copycat/Serializer` containing the `SerializerFactory`
implementation class name.

```java
public class JacksonSerializerFactory extends SerializerFactory {

  @Override
  public Serializer createSerializer() {
    return new JacksonSerializer();
  }

}
```

The serializer factory should return a `Serializer` instance via the `createSerializer` method.
This is the basic default serializer used by CopyCat:

```java
public class JacksonSerializer implements Serializer {
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public byte[] writeValue(Object value) {
    try {
      return mapper.writeValueAsBytes(value);
    } catch (JsonProcessingException e) {
      throw new SerializationException(e.getMessage());
    }
  }

  @Override
  public <T> T readValue(byte[] bytes, Class<T> type) {
    try {
      return mapper.readValue(bytes, type);
    } catch (IOException e) {
      throw new SerializationException(e.getMessage());
    }
  }

}
```

# Protocols
CopyCat is an abstract API that can implement the Raft consensus algorithm over
any conceivable protocol. To do this, CopyCat provides a flexible protocol plugin
system. Protocols use special URIs - such as `direct:foo` or `tcp://localhost:5050` -
and CopyCat uses a custom service loader similar to the Java service loader. Using
URIs, a protocol can be constructed and started by CopyCat without the large amounts
of boilerplate code that would otherwise be required.

To define a new protocol, simply create a file in your project's
`META-INF/services/net/kuujo/copycat/protocol` directory, naming the file with the
project name. In the file should be a single string indicating the name of the protocol
class. For example:

`META-INF/services/net/kuujo/copycat/protocol/http`

```
net.kuujo.copycat.protocol.impl.HttpProtocol
```

The class name should point to a class that implements the `Protocol` interface.
The `Protocol` interface provides the following methods:

```java
public interface Protocol {

  void init(CopyCatContext context);

  ProtocolClient createClient();

  ProtocolServer createServer();

}
```

You'll notice that the `Protocol` itself doesn't actually do anything. Instead,
it simply provides factories for `ProtocolClient` and `ProtocolServer`.

### Writing a protocol server
The `ProtocolServer` interface is implemented by the receiving side of the protocol.
The server's task is quite simple - to call replica callbacks when a message is received.
In order to do so, the `ProtocolServer` provides a number of methods for registering
callbacks for each command. Each callback is in the form of an `AsyncCallback` instance.

```java
public interface ProtocolServer {

  void protocolHandler(ProtocolHandler handler);

  void start(AsyncCallback<Void> callback);

  void stop(AsyncCallback<Void> callback);

}
```

Each `ProtocolServer` should only ever have a single callback for each method registered
at any given time. When the `start` and `stop` methods are called, the server should
obviously start and stop servicing requests respectively. It's up to the server to
transform wire-level messages into the appropriate `Request` instances.

The `ProtocolHandler` to which the server calls implements the same interface as
`ProtocolClient` below.

### Writing a protocol client
The client's task is equally simple - to send messages to another replica when asked.
In order to do so, the `ProtocolClient` implements the other side of the `ProtocolServer`
callback methods.

```java
public interface ProtocolClient {

  void appendEntries(AppendEntriesRequest request, AsyncCallback<AppendEntriesResponse> callback);

  void requestVote(RequestVoteRequest request, AsyncCallback<RequestVoteResponse> callback);

  void submitCommand(SubmitCommandRequest request, AsyncCallback<SubmitCommandResponse> callback);

}
```

### Injecting URI arguments into a protocol
You may be wondering how you can get URI arguments into your protocol implementation.
CopyCat provides special annotations that can be used to inject specific URI parts
into your protocol. Each URI annotation directly mirrors a method on the Java `URI`
interface, so users can extract any information necessary out of the protocol URI.

These are the available URI annotations:
* `@UriScheme`
* `@UriSchemeSpecificPart`
* `@UriUserInfo`
* `@UriHost`
* `@UriPort`
* `@UriAuthority`
* `@UriPath`
* `@UriQuery`
* `@UriQueryParam`
* `@UriFragment`

Each of these annotations mirrors a method on the `URI` interface except for the
last one, `@UriQueryParam`. The `@UriQueryParam` annotation is a special annotation
for referencing parsed named query arguments.

URI annotations can be used either on protocol constructors or setter methods.
In either case, constructors or methods *must first be annotated with the
`@UriInject` annotation* in order to enable URI injection. Let's take a look
at an example of constructor injection:

```java
public class HttpProtocol implements Protocol {
  private final String host;
  private final int port;
  private final String path;

  @UriInject
  public HttpProtocol(@UriHost String host, @UriPort int port @UriPath String path) {
    this.host = host;
    this.port = port;
    this.path = path;
  }

}
```

When the protocol instance is first constructed, the CopyCat `UriInjector` will
find any constructors with the `@UriInject` annotation and attempt to construct
the object using that constructor. Note that if the construction fails, the injector
will then try to fall back to a no-argument constructor. If a no argument constructor
exists then a `ProtocolException` will be thrown.

The CopyCat URI injector also supports multiple constructors. This can be useful
for when there are several ways to construct the same object. For instance, we may
be constructing a `HttpClient` instance within our `HttpProtocol` constructor. We
can then create two constructors, one accepting a `host` and a `port` and one accepting
a `client`.

```java
public class HttpProtocol implements Protocol {
  private final HttpClient client;
  private final String path;

  @UriInject
  public HttpProtocol(@UriQueryParam("client") HttpClient client, @UriPath String path) {
    this.client = client;
    this.path = path;
  }

  @UriInject
  public HttpProtocol(@UriHost String host, @UriPort int port @UriPath String path) {
    this(new HttpClient(host, port), path);
  }

}
```

You may be interested in how CopyCat decides which constructor to use. Actually, it's
quite simple: the URI injector simply iterates over `@UriInject` annotated constructors
and attempts to construct the object from each one. If a given constructor cannot be
used due to a missing argument (such as the named `@UriQueryParam("client")`), the constructor
will be skipped. Once all constructors have been exhausted, the injector will again attempt
to fall back to a no-argument constructor.

Note also that the `@UriQueryParam("client")` annotation is referencing a `HttpClient` object
which obviously can't exist within a raw URI string. Users can use a `Registry` instance
to register named objects that can then be referenced in URIs using the `$` prefix. For
instance:

```java
Registry registry = new BasicRegistry();
registry.bind("http_client", new HttpClient("localhost", 8080));
String uri = "http://copycat?client=$http_client";
```

The registry can then be passed to a `CopyCatContext` constructor.

```java
CopyCatContext context = new CopyCatContext(new MyStateMachine, cluster, registry);
```

When the URI query string is parsed, the parser will look for strings beginning with `$`
and use those strings to look up referenced objects in the context's registry.

### Using multiple URI annotations on a single parameter

URI schemas can often be inflexible for this type of use case, and users may want to
be able to back a parameter with multiple annotations. These two URIs will not parse
in the same way:

* `http:copycat`
* `http://copycat`

In the first example, the `copycat` path can be fetch via `URI.getSchemeSpecificPart()`,
but the second example requires `URI.getAuthority()`. CopyCat supports multiple URI
annotations on a single parameter. Annotations will be evaluated from left to right.
So, if the first annotation is evaluated, and no matching (non-null) argument is found,
the injector will look for another annotation. This can be used to create some order
of importance. For instance, in the example above, we would want to use the
`@UriAuthority` annotation first, since in either case the `@UriSchemeSpecificPart`
will not be null.

```java
@UriInject
public HttpProtocol(@UriAuthority @UriSchemeSpecificPart String path) {
  this.path = path;
}
```

### Making annotated URI parameters optional
In order to allow for more control over the way CopyCat selects constructors, users
can use the `@Optional` annotation to indicate that a `null` parameter can be ignored
if necessary. This will prevent CopyCat from skipping otherwise successful constructors.
For instance, in our `HttpProtocol` example, the constructor could certainly take a
`host` without a `port`. Of course, we could simply create another constructor, but
maybe we just don't wan to :-)

```java
public class HttpProtocol implements Protocol {

  @UriInject
  public HttpProtocol(@UriHost String host, @Optional @UriPort int port) {
    this.host = host;
    this.port = port >= 0 ? port : 0;
  }

}
```

### Injecting URI arguments via annotated setters
Just as constructors support annotated parameters, protocols can also support URI injection
via bean properties. To use annotated setters, simply annotate the setter with any URI
injectable annotation.

```java
public class HttpProtocol implements Protocol {
  private String host = "localhost";

  @UriHost
  public void setHost(String host) {
    this.host = host;
  }

  public String getHost() {
    return host;
  }

}
```

All setter injected URI arguments are optional, so you should ensure that required arguments
have been injected prior to constructing client or server instances.

The setter injector also supports injecting arbitrary bean properties. Any bean setters
will be injected with named query parameters.

```java
public class HttpProtocol implements Protocol {
  private long timeout;

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  public long getTimeout() {
    return timeout;
  }

}
```

When this class is injected with a URI like `http://localhost:8080?timeout=1000` the
`timeout` will automatically be injected based on the matching bean descriptor. This also
works with registered objects.

### The complete protocol
Now that we have all that out of the way, here's the complete `Protocol` implementation:

```java
public class HttpProtocol implements Protocol {
  private HttpClient client;
  private HttpServer server;
  private String path;

  @UriInject
  public HttpProtocol(@UriQueryParam("client") HttpClient client, @UriQueryParam("server") HttpServer server @UriAuthority String path) {
    this.client = client;
    this.server = server;
    this.path = path;
  }

  @UriInject
  public HttpProtocol(@UriHost String host, @Optional @UriPort int port, @UriPath String path) {
    this.client = new HttpClient(host, port);
    this.server = new HttpServer(host, port);
    this.path = path;
  }

  @Override
  public void init(CopyCatContext context) {
  }

  @Override
  public ProtocolClient createClient() {
    return new HttpProtocolClient(client, path);
  }

  @Override
  public ProtocolServer createServer() {
    return new HttpProtocolServer(server, path);
  }

}
```

### Configuring the cluster with custom protocols

Let's take a look at an example of how to configure the CopyCat cluster when using
custom protocols.

```java
ClusterConfig cluster = new ClusterConfig("http://localhost:8080/copycat");
cluster.addRemoteMember("http://localhost:8081/copycat");
cluster.addRemoteMember("http://localhost:8082/copycat");
```

Note that CopyCat does not particularly care about the protocol of any given node
in the cluster. Theoretically, different nodes could be connected together by any
protocol they want (though I can't imagine why one would want to do such a thing).
For the local replica, the protocol's server is used to receive messages. For remote
replicas, each protocol instance's client is used to send messages to those replicas.

## Built-in protocols
CopyCat maintains several built-in protocols, some of which are implemented on top
of asynchronous frameworks like [Netty](http://netty.io) and [Vert.x](http://vertx.io).

### Direct Protocol
The `direct` protocol is a simple protocol that communicates between contexts using
direct method calls. This protocol is intended purely for testing.

```java
Registry registry = new ConcurrentRegistry();
ClusterConfig cluster = new ClusterConfig("direct:foo");
cluster.setRemoteMembers("direct:bar", "direct:baz");
CopyCatContext context = new CopyCatContext(new MyStateMachine, cluster, registry);
```

Note that you should use a `ConcurrentRegistry` when using the `direct` protocol.

### Netty TCP Protocol
The netty `tcp` protocol communicates between replicas using Netty TCP channels.

In order to use Netty protocols, you must add the `copycat-netty` project as a
dependency. Once the `copycat-netty` library is available on your classpath,
CopyCat will automatically find the Netty `tcp` protocol.

```java
ClusterConfig cluster = new ClusterConfig("tcp://localhost:1234");
```

### Vert.x Event Bus Protocol
The Vert.x `eventbus` protocol communicates between replicas on the Vert.x event
bus. The event bus can either be created in a new `Vertx` instance or referenced
in an existing `Vertx` instance.

In order to use Vert.x protocols, you must add the `copycat-vertx` project as a dependency.

```java
ClusterConfig cluster = new ClusterConfig("eventbus://localhost:1234/foo");

// or...

ClusterConfig cluster = new ClusterConfig("eventbus://foo?vertx=$vertx");
Registry registry = new BasicRegistry();
registry.bind("vertx", vertx);
```

### Vert.x TCP Protocol
The Vert.x `tcp` protocol communicates between replicas using a simple wire protocol
over Vert.x `NetClient` and `NetServer` instances. To configure the `tcp` protocol,
simply use an ordinary TCP address.

In order to use Vert.x protocols, you must add the `copycat-vertx` project as a dependency.

```java
ClusterConfig cluster = new ClusterConfig("tcp://localhost:1234");
cluster.setRemoteMembers("tcp://localhost:1235", "tcp://localhost:1236");
```

# Endpoints
We've gone through developing custom protocols for communicating between
nodes in CopyCat, but a replicated state machine isn't much good if you can't
get much information into it. Nodes need some sort of API that can be exposed to
the outside world. For this, CopyCat provides an *endpoints* API that behaves very
similarly to the *protocol* API.

To register an endpoint, simply add a file to the `META-INF/services/net/kuujo/copycat/endpoints`
directory, using the endpoint name as the file name. The file should contain a string referencing
a class that implements the `Endpoint` interface.

The `Endpoint` interface is very simple:

```java
public interface Endpoint {

  void init(CopyCatContext context);

  void start(AsyncCallback<Void> callback);

  void stop(AsyncCallback<Void> callback);

}
```

Endpoints simply wrap the `CopyCatContext` and forward requests to the local
context via the `CopyCatContext.submitCommand` method.

### Using URI annotations with endpoints
Endpoints support all the same URI annotations as do protocols. See the protocol
[documentation on URI annotations](#injecting-uri-arguments-into-a-protocol)
for a tutorial on injecting arguments into custom endpoints.

### Wrapping the CopyCatContext in an endpoint service
CopyCat provides a simple helper class for wrapping a `CopyCatContext in an
endpoint. To wrap a context, use the `CopyCat` class. The `CopyCat` constructor
simply accepts a single additional argument which is the endpoint URI.

```java
ClusterConfig cluster = new ClusterConfig("tcp://localhost:5555", "tcp://localhost:5556", "tcp://localhost:5557");
CopyCat copycat = new CopyCat("http://localhost:8080", new MyStateMachine(), cluster)
copycat.start();
```

## Built-in endpoints
CopyCat also provides a couple of built-in endpoints via the `copycat-vertx`
project. In order to use Vert.x endpoints, you must add the `copycat-vertx`
project as a dependency.

### Vert.x Event Bus Endpoint
The Vert.x event bus endpoint receives submissions over the Vert.x event bus.

```java
CopyCat copycat = new CopyCat("eventbus://localhost:8080/foo", new MyStateMachine(), cluster);

// or...

Registry registry = new BasicRegistry();
registry.bind("vertx", vertx);
CopyCat copycat = new CopyCat("eventbus://foo?vertx=$vertx", new MyStateMachine(), cluster, registry);
```

### Vert.x TCP Endpoint
The Vert.x TCP endpoint uses a delimited JSON-based protocol:
```java
CopyCat copycat = new CopyCat("tcp://localhost:8080", new MyStateMachine(), cluster);
```

### Vert.x HTTP Endpoint
The Vert.x HTTP endpoint uses a simple HTTP server which accepts JSON `POST` requests
to the command path. For instance, to submit the `read` command with `{"key":"foo"}`
execute a `POST` request to the `/read` path using a JSON body containing command arguments.

```java
CopyCat copycat = new CopyCat("http://localhost:8080", new MyStateMachine(), cluster);
```

Tutorials
=========

1. [A simple fault-tolerant key-value store](#writing-a-simple-fault-tolerant-key-value-store)
   * [Creating the state machine](#creating-the-state-machine)
   * [Adding state machine commands](#adding-state-machine-commands)
   * [Configuring the cluster](#configuring-the-cluster)
   * [Setting up the log](#setting-up-the-log)
   * [Starting the replica](#starting-the-replica)
1. [Improving the key-value store with custom protocols and endpoints](#improving-the-key-value-store-with-custom-protocols-and-endpoints)
   * [Creating the TCP protocol](#creating-the-tcp-protocol)
      * [Creating URI-based constructors](#creating-uri-based-constructors)
      * [Writing the protocol server](#writing-the-protocol-server)
      * [Writing the protocol client](#writing-the-protocol-client)
   * [Creating the HTTP endpoint](#creating-the-http-endpoint)
      * [Creating URI-based constructors](#creating-uri-based-constructors)
      * [Writing the HTTP server](#writing-the-http-server)
   * [Running the key-value-store](#running-the-key-value-store)


## A simple fault-tolerant key-value store
The simplest example of a distributed, fault-tolerant database is a key-value
store. While not necessarily efficient for the use case, CopyCat can be used
to quickly write such a system in only a few lines of code.

### Creating the state machine
State machines are created by implementing the `StateMachine` interface. However,
most users will find it useful to use the state machine helper - `AnnotatedStateMachine`.
The `AnnotatedStateMachine` is an abstract class that supports annotation based
state machine configurations.

State machines are objects on which arbitrary commands can be called in the
form of RPCs. Given the same commands in the same order, the state machine should
always arrive at the same state with the same output. This is essential to maintaining
consistency of state across the cluster. In general, this means state machine should
not access external data sources that change over time.

```java
public class KeyValueStore extends AnnotatedStateMachine {

  @Stateful
  private final Map<String, Object> data = new HashMap<>();

}
```

Note here that we use the `@Stateful` annotation. When replicates logs become too
large, CopyCat will take and persist a snapshot of the state machine's state, allowing
it to remove irrelevant entries from the log. The `@Stateful` annotation indicates
to CopyCat that the annotated field should be included in the state machine snapshot
whenever it is taken.

### Adding state machine commands
Commands on the `AnnotatedStateMachine` are defined by the `@Command` annotation. Each
`@Command` should have a `name` assigned to it. The command's `name` is the name by
which users executing commands on the state machine will reference the annotated method.

Additionally, the `@Command` annotation supports an optional `type` argument. This
argument indicates to CopyCat how the command should be handled internally. If the
command is a `WRITE` or `READ_WRITE` command, CopyCat will log and replicate the command
before applying it to the state machine. Alternatively, if the command is a `READ` command
then CopyCat will skip logging the command since it has no effect on the state machine state.

Each command can have any number of arguments annotated by the `@Argument` annotation. When
a command is submitted to the CopyCat cluster, the user is allowed to provide arbitrary
named arguments. The `AnnotatedStateMachine` will automatically validate argument types
against user provided arguments according to parameter annotations.

```java
public class KeyValueStore extends AnnotatedStateMachine {

  @Stateful
  private final Map<String, Object> data = new HashMap<>();

  @Command(name = "get", type = Command.Type.READ)
  public Object get(@Argument("key") String key) {
    return data.get(key);
  }

  @Command(name = "set", type = Command.Type.WRITE)
  public void set(@Argument("key") String key, @Argument("value") Object value) {
    data.put(key, value);
  }

  @Command(name = "delete", type = Command.Type.WRITE)
  public void delete(@Argument("key") String key) {
    data.remove(key);
  }

}
```

That's it. We've just implemented a fault-tolerant in-memory key-value store.
Now let's see how to run it.

### Configuring the cluster
CopyCat requires explicitly defined cluster configurations in order to perform
log replication. Nodes in CopyCat are defined by special URIs which reference the
protocol over which the node communicates.

```java
ClusterConfig cluster = new ClusterConfig();
cluster.setLocalMember("tcp://localhost:5555");
cluster.setRemoteMembers("tcp://localhost:5556", "tcp://localhost:5557");
```

In this case, we're using a TCP-based protocol. Remember that CopyCat core does
not implement any network-based protocols (it does provide a test `direct` protocol),
but additional projects provide protocol implementations. See the section on
[protocols](#protocols) for more info.

### Setting up the log
By default, CopyCat replicas use an in-memory log combined with snapshotting to
log and replicate events. However, this behavior can be changed by contructing
another `Log` type.

```java
Log log = new FileLog("key-value.log");
```

### Starting the replica
Now that we've created our state machine and configured the cluster and log
we can start the local replica.

```java
CopyCatContext context = new CopyCatContext(new KeyValueStore(), log, cluster);
context.start();
```

Note that this example only demonstrates how to start a single node. In a
real-world scenario this snippet would need to be run on several nodes in a cluster.

Once the node has been started, we can begin submitting commands.

```java
String command = "set";
Map<String, Object> args = new HashMap<>();
args.put("key", "test");
args.put("value", "Hello world!");

context.submitCommand(command, args, result -> {
  if (result.succeeded()) {
    Object value = result.value();
  }
});
```

Or, in Java 7...

```java
String command = "set";
Map<String, Object> args = new HashMap<>();
args.put("key", "test");
args.put("value", "Hello world!");

context.submitCommand(command, args, new AsyncCallback<Object>() {
  public void call(AsyncResult<Object> result) {
    if (result.succeeded()) {
      Object value = result.value();
    }
  }
});
```

## Improving the key-value store with custom protocols and endpoints
At its core, CopyCat is an implementation of the Raft consensus algorithm but
without a transport. However, CopyCat does provide a pluggable API for users to
implement their own transport layer. This section will demonstrate how to implement
custom protocols and endpoints for CopyCat using the Vert.x platform as an example.

Protocols and endpoints are two sides of the same coin. Both are simple communication
channels, but they differ widely in their use.

**Protocols** are used by CopyCat internally to communicate between replicas.
The CopyCat `Protocol` API is essentially an implementation of RPCs from the
Raft consensus protocol. While the protocol API can be user defined, it is only
ever called from within CopyCat. This means that performance is critical over usability.

**Endpoints** are user-facing servers for the replicated state machine. Whereas the
previous tutorial demonstrated how to create and start a single node in a CopyCat cluster,
endpoints can be used to access the cluster over the network. This means that usability
can be important if the endpoint is implemented over HTTP, for instance.

### Creating the TCP protocol
Since performance is critical for protocol impelmentations, we're going to implement
the key-value store protocol over TCP using Vert.x. We'll call the protocol `tcp`.

To create a custom protocol, we first need to register the protocol class in the
`META-INF/services/net/kuujo/copycat/protocol` directory.

`META-INF/services/net/kuujo/copycat/protocol/tcp`

```
net.kuujo.copycat.protocol.impl.TcpProtocol
```

The class contained within the service file is a `Protocol` implementation.

```java
public class TcpProtocol implements Protocol {
  private final String host;
  private final int port;
  private final Vertx vertx;

  public TcpProtocol(String host, int port, Vertx vertx) {
    this.host = host;
    this.port = port;
    this.vertx = vertx;
  }

  @Override
  public void init(CopyCatContext context) {
  }

  @Override
  public ProtocolServer createServer() {
    return new TcpProtocolServer(vertx, host, port);
  }

  @Override
  public ProtocolClient createClient() {
    return new TcpProtocolClient(vertx, host, port);
  }

}
```

The `Protocol` interface is really just a factory interface for protocol clients
and servers. CopyCat will handle constructing clients and servers according to the
needs of the given node. For instance, the local node will construct a server on
which to receive messages from other nodes and a client for each replica to which
is sends messages.

#### Creating URI-based constructors
CopyCat protocols are identified by URIs, and as such CopyCat provides facilities
for injecting URI arguments into the `Protocol` implementation via cosntructors.

```java
public class TcpProtocol implements Protocol {
  private final String host;
  private final int port;
  private final Vertx vertx;

  @UriInject
  public TcpProtocol(@UriHost String host, @UriPort int port) {
    this.host = host;
    this.port = port;
    this.vertx = new DefaultVertx();
  }

}
```

When the `Protocol` instance is created, CopyCat will parse the protocol URI
and pass the URI host and port as the appropriate constructor arguments. For more
information on URI injection see
[Injecting URI arguments into a protocol](#injecting-uri-arguments-into-a-protocol).

#### Writing the protocol server
Protocol servers will be constructed by the local node in order to receive messages
from other nodes in the cluster. To create a protocol server, implement the `ProtocolServer`
interface.

```java
public class TcpProtocolServer implements ProtocolServer {
  private static final Serializer serializer = SerializerFactory.getSerializer();
  private final Vertx vertx;
  private final String host;
  private final int port;
  private NetServer server;
  private ProtocolHandler requestHandler;

  public TcpProtocolServer(Vertx vertx, String host, int port) {
    this.vertx = vertx;
    this.host = host;
    this.port = port;
  }

  @Override
  public void protocolHandler(ProtocolHandler handler) {
    this.requestHandler = handler;
  }

  @Override
  public void start(final AsyncCallback<Void> callback) {
    if (server == null) {
      server = vertx.createNetServer();
      server.connectHandler(new Handler<NetSocket>() {
        @Override
        public void handle(final NetSocket socket) {
          socket.dataHandler(RecordParser.newDelimited(new byte[]{'\0'}, new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
              JsonObject request = new JsonObject(buffer.toString());
              String type = request.getString("type");
              if (type != null) {
                switch (type) {
                  case "append":
                    handleAppendRequest(socket, request);
                    break;
                  case "vote":
                    handleVoteRequest(socket, request);
                    break;
                  case "submit":
                    handleSubmitRequest(socket, request);
                    break;
                  default:
                    respond(socket, new JsonObject().putString("status", "error").putString("message", "Invalid request type"));
                    break;
                }
              } else {
                respond(socket, new JsonObject().putString("status", "error").putString("message", "Invalid request type"));
              }
            }
          }));
        }
      }).listen(port, host, new Handler<AsyncResult<NetServer>>() {
        @Override
        public void handle(AsyncResult<NetServer> result) {
          if (result.failed()) {
            callback.call(new net.kuujo.copycat.AsyncResult<Void>(result.cause()));
          } else {
            callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
          }
        }
      });
    } else {
      callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
    }
  }

  /**
   * Handles an append entries request.
   */
  private void handleAppendRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final Object id = request.getValue("id");
      List<Entry> entries = new ArrayList<>();
      JsonArray jsonEntries = request.getArray("entries");
      if (jsonEntries != null) {
        for (Object jsonEntry : jsonEntries) {
          entries.add(serializer.readValue(jsonEntry.toString().getBytes(), Entry.class));
        }
      }
      requestHandler.appendEntries(new AppendEntriesRequest(id, request.getLong("term"), request.getString("leader"), request.getLong("prevIndex"), request.getLong("prevTerm"), entries, request.getLong("commit")), new AsyncCallback<AppendEntriesResponse>() {
        @Override
        public void call(net.kuujo.copycat.AsyncResult<AppendEntriesResponse> result) {
          if (result.succeeded()) {
            AppendEntriesResponse response = result.value();
            if (response.status().equals(Response.Status.OK)) {
              respond(socket, new JsonObject().putString("status", "ok").putValue("id", id).putNumber("term", response.term()).putBoolean("succeeded", response.succeeded()));
            } else {
              respond(socket, new JsonObject().putString("status", "error").putValue("id", id).putString("message", response.error().getMessage()));
            }
          } else {
            respond(socket, new JsonObject().putString("status", "error").putValue("id", id).putString("message", result.cause().getMessage()));
          }
        }
      });
    }
  }

  /**
   * Handles a vote request.
   */
  private void handleVoteRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final Object id = request.getValue("id");
      requestHandler.requestVote(new RequestVoteRequest(id, request.getLong("term"), request.getString("candidate"), request.getLong("lastIndex"), request.getLong("lastTerm")), new AsyncCallback<RequestVoteResponse>() {
        @Override
        public void call(net.kuujo.copycat.AsyncResult<RequestVoteResponse> result) {
          if (result.succeeded()) {
            RequestVoteResponse response = result.value();
            if (response.status().equals(Response.Status.OK)) {
              respond(socket, new JsonObject().putString("status", "ok").putValue("id", id).putNumber("term", response.term()).putBoolean("voteGranted", response.voteGranted()));
            } else {
              respond(socket, new JsonObject().putString("status", "error").putValue("id", id).putString("message", response.error().getMessage()));
            }
          } else {
            respond(socket, new JsonObject().putString("status", "error").putValue("id", id).putString("message", result.cause().getMessage()));
          }
        }
      });
    }
  }

  /**
   * Handles a submit request.
   */
  private void handleSubmitRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final Object id = request.getValue("id");
      requestHandler.submitCommand(new SubmitCommandRequest(id, request.getString("command"), request.getObject("args").toMap()), new AsyncCallback<SubmitCommandResponse>() {
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void call(net.kuujo.copycat.AsyncResult<SubmitCommandResponse> result) {
          if (result.succeeded()) {
            SubmitCommandResponse response = result.value();
            if (response.status().equals(Response.Status.OK)) {
              if (response.result() instanceof Map) {
                respond(socket, new JsonObject().putString("status", "ok").putValue("id", id).putObject("result", new JsonObject((Map) response.result())));
              } else if (response.result() instanceof List) {
                respond(socket, new JsonObject().putString("status", "ok").putValue("id", id).putArray("result", new JsonArray((List) response.result())));
              } else {
                respond(socket, new JsonObject().putString("status", "ok").putValue("id", id).putValue("result", response.result()));
              }
            } else {
              respond(socket, new JsonObject().putString("status", "error").putValue("id", id).putString("message", response.error().getMessage()));
            }
          } else {
            respond(socket, new JsonObject().putString("status", "error").putValue("id", id).putString("message", result.cause().getMessage()));
          }
        }
      });
    }
  }

  /**
   * Responds to a request from the given socket.
   */
  private void respond(NetSocket socket, JsonObject response) {
    socket.write(response.encode() + '\0');
  }

  @Override
  public void stop(final AsyncCallback<Void> callback) {
    if (server != null) {
      server.close(new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            callback.call(new net.kuujo.copycat.AsyncResult<Void>(result.cause()));
          } else {
            callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
          }
        }
      });
    } else {
      callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
    }
  }

}
```

#### Writing the protocol client
Protocol clients will be constructed by each node in order to send messages to
remote nodes.

```java
public class TcpProtocolClient implements ProtocolClient {
  private static final Serializer serializer = SerializerFactory.getSerializer();
  private final Vertx vertx;
  private final String host;
  private final int port;
  private NetClient client;
  private NetSocket socket;
  private final Map<Object, ResponseHolder> responses = new HashMap<>();

  /**
   * Holder for response handlers.
   */
  @SuppressWarnings("rawtypes")
  private static class ResponseHolder {
    private final AsyncCallback callback;
    private final ResponseType type;
    private final long timer;
    private ResponseHolder(long timerId, ResponseType type, AsyncCallback callback) {
      this.timer = timerId;
      this.type = type;
      this.callback = callback;
    }
  }

  /**
   * Indicates response types.
   */
  private static enum ResponseType {
    APPEND,
    VOTE,
    SUBMIT;
  }

  public TcpProtocolClient(Vertx vertx, String host, int port) {
    this.vertx = vertx;
    this.host = host;
    this.port = port;
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncCallback<AppendEntriesResponse> callback) {
    if (socket != null) {
      JsonArray jsonEntries = new JsonArray();
      for (Entry entry : request.entries()) {
        jsonEntries.addString(new String(serializer.writeValue(entry)));
      }
      socket.write(new JsonObject().putString("type", "append")
          .putValue("id", request.id())
          .putNumber("term", request.term())
          .putString("leader", request.leader())
          .putNumber("prevIndex", request.prevLogIndex())
          .putNumber("prevTerm", request.prevLogTerm())
          .putArray("entries", jsonEntries)
          .putNumber("commit", request.commitIndex()).encode() + '\00');
      storeCallback(request.id(), ResponseType.APPEND, callback);
    } else {
      callback.call(new net.kuujo.copycat.AsyncResult<AppendEntriesResponse>(new ProtocolException("Client not connected")));
    }
  }

  @Override
  public void requestVote(RequestVoteRequest request, AsyncCallback<RequestVoteResponse> callback) {
    if (socket != null) {
      socket.write(new JsonObject().putString("type", "vote")
          .putValue("id", request.id())
          .putNumber("term", request.term())
          .putString("candidate", request.candidate())
          .putNumber("lastIndex", request.lastLogIndex())
          .putNumber("lastTerm", request.lastLogTerm())
          .encode() + '\00');
      storeCallback(request.id(), ResponseType.VOTE, callback);
    } else {
      callback.call(new net.kuujo.copycat.AsyncResult<RequestVoteResponse>(new ProtocolException("Client not connected")));
    }
  }

  @Override
  public void submitCommand(SubmitCommandRequest request, AsyncCallback<SubmitCommandResponse> callback) {
    if (socket != null) {
      socket.write(new JsonObject().putString("type", "submit")
          .putValue("id", request.id())
          .putString("command", request.command())
          .putObject("args", new JsonObject(request.args()))
          .encode() + '\00');
      storeCallback(request.id(), ResponseType.SUBMIT, callback);
    } else {
      callback.call(new net.kuujo.copycat.AsyncResult<SubmitCommandResponse>(new ProtocolException("Client not connected")));
    }
  }

  /**
   * Handles an identifiable response.
   */
  @SuppressWarnings("unchecked")
  private void handleResponse(Object id, JsonObject response) {
    ResponseHolder holder = responses.remove(id);
    if (holder != null) {
      vertx.cancelTimer(holder.timer);
      switch (holder.type) {
        case APPEND:
          handleAppendResponse(response, (AsyncCallback<AppendEntriesResponse>) holder.callback);
          break;
        case VOTE:
          handleVoteResponse(response, (AsyncCallback<RequestVoteResponse>) holder.callback);
          break;
        case SUBMIT:
          handleSubmitResponse(response, (AsyncCallback<SubmitCommandResponse>) holder.callback);
          break;
      }
    }
  }

  /**
   * Handles an append entries response.
   */
  private void handleAppendResponse(JsonObject response, AsyncCallback<AppendEntriesResponse> callback) {
    String status = response.getString("status");
    if (status == null) {
      callback.call(new net.kuujo.copycat.AsyncResult<AppendEntriesResponse>(new ProtocolException("Invalid response")));
    } else if (status.equals("ok")) {
      callback.call(new net.kuujo.copycat.AsyncResult<AppendEntriesResponse>(new AppendEntriesResponse(response.getValue("id"), response.getLong("term"), response.getBoolean("succeeded"))));
    } else if (status.equals("error")) {
      callback.call(new net.kuujo.copycat.AsyncResult<AppendEntriesResponse>(new ProtocolException(response.getString("message"))));
    }
  }

  /**
   * Handles a vote response.
   */
  private void handleVoteResponse(JsonObject response, AsyncCallback<RequestVoteResponse> callback) {
    String status = response.getString("status");
    if (status == null) {
      callback.call(new net.kuujo.copycat.AsyncResult<RequestVoteResponse>(new ProtocolException("Invalid response")));
    } else if (status.equals("ok")) {
      callback.call(new net.kuujo.copycat.AsyncResult<RequestVoteResponse>(new RequestVoteResponse(response.getValue("id"), response.getLong("term"), response.getBoolean("voteGranted"))));
    } else if (status.equals("error")) {
      callback.call(new net.kuujo.copycat.AsyncResult<RequestVoteResponse>(new ProtocolException(response.getString("message"))));
    }
  }

  /**
   * Handles a submit response.
   */
  private void handleSubmitResponse(JsonObject response, AsyncCallback<SubmitCommandResponse> callback) {
    String status = response.getString("status");
    if (status == null) {
      callback.call(new net.kuujo.copycat.AsyncResult<SubmitCommandResponse>(new ProtocolException("Invalid response")));
    } else if (status.equals("ok")) {
      callback.call(new net.kuujo.copycat.AsyncResult<SubmitCommandResponse>(new SubmitCommandResponse(response.getValue("id"), response.getObject("result").toMap())));
    } else if (status.equals("error")) {
      callback.call(new net.kuujo.copycat.AsyncResult<SubmitCommandResponse>(new ProtocolException(response.getString("message"))));
    }
  }

  /**
   * Stores a response callback by ID.
   */
  private <T extends Response> void storeCallback(final Object id, ResponseType responseType, AsyncCallback<T> callback) {
    long timerId = vertx.setTimer(30000, new Handler<Long>() {
      @Override
      @SuppressWarnings("unchecked")
      public void handle(Long timerID) {
        ResponseHolder holder = responses.remove(id);
        if (holder != null) {
          holder.callback.call(new net.kuujo.copycat.AsyncResult<T>(new ProtocolException("Request timed out")));
        }
      }
    });
    ResponseHolder holder = new ResponseHolder(timerId, responseType, callback);
    responses.put(id, holder);
  }

  @Override
  public void connect() {
    connect(null);
  }

  @Override
  public void connect(final AsyncCallback<Void> callback) {
    if (client == null) {
      client = vertx.createNetClient();
      client.connect(port, host, new Handler<AsyncResult<NetSocket>>() {
        @Override
        public void handle(AsyncResult<NetSocket> result) {
          if (result.failed()) {
            if (callback != null) {
              callback.call(new net.kuujo.copycat.AsyncResult<Void>(result.cause()));
            }
          } else {
            socket = result.result();
            socket.dataHandler(RecordParser.newDelimited(new byte[]{'\00'}, new Handler<Buffer>() {
              @Override
              public void handle(Buffer buffer) {
                JsonObject response = new JsonObject(buffer.toString());
                Object id = response.getValue("id");
                handleResponse(id, response);
              }
            }));
            if (callback != null) {
              callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
            }
          }
        }
      });
    } else if (callback != null) {
      callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
    }
  }

  @Override
  public void close() {
    close(null);
  }

  @Override
  public void close(final AsyncCallback<Void> callback) {
    if (client != null && socket != null) {
      socket.closeHandler(new Handler<Void>() {
        @Override
        public void handle(Void event) {
          socket = null;
          client.close();
          client = null;
          if (callback != null) {
            callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
          }
        }
      }).close();
    } else if (client != null) {
      client.close();
      client = null;
      if (callback != null) {
        callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
      }
    } else if (callback != null) {
      callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
    }
  }

}
```

Note that CopyCat uses correlation identifiers to correlate requests and responses,
and it's important that protocol implementations respect this correlation. Do not
generate your own correlation IDs. Correlation identifiers are generated by a
`CorrelationStrategy` which is configured in the `CopyCatConfig`.

### Creating the HTTP endpoint
Endpoints are simply servers that listen for requests from the outside worl.
When an endpoint receives a request, it should forward that request on to the
CopyCat cluster by making a call to `submitCommand` on the local `CopyCatContext`.
When a command is submitted to the cluster, CopyCat will handle routing the
command to the current cluster leader internally.

To create an endpoint, we again need to register the service by creating a
file at `META-INF/services/net/kuujo/copycat/endpoint`.

`META-INF/services/net/kuujo/copycat/endpoint/http`

```
net.kuujo.copycat.endpoint.impl.HttpEndpoint
```

The `HttpEndpoint` class that we registered is an implementation of the
`Endpoint` interface.

```java
public class HttpEndpoint implements Endpoint {
  private CopyCatContext context;

  @Override
  public void init(CopyCatContext context) {
    this.context = context;
  }

  @Override
  public void start(AsyncCallback<Void> callback) {
  }

  @Override
  public void stop(AsyncCallback<Void> callback) {
  }

}
```

Note that the only API calls are calls that CopyCat makes to initialize and
start the endpoint. It is the responsibility of the endpoint to forward messages
on to the CopyCat cluster via the `CopyCatContext`.

#### Creating URI-based constructors
As with protocols, endpoints can be constructed through URIs, and CopyCat's
URI injection facilities can be used to inject URI arguments into the endpoint's
constructors.

```java
public class HttpEndpoint implements Endpoint {
  private final String host;
  private final int port;
  private final Vertx vertx;

  @UriInject
  public HttpEndpoint(@UriHost String host, @Optional @UriPort int port) {
    this.host = host;
    this.port = port;
    this.vertx = new DefaultVertx();
  }

}
```

#### Writing the HTTP server
A properly functioning endpoint will start a server and listen for commands
to forward to the CopyCat cluster. This is done by simply parsing requests
into a `command` and arguments and calling `submitCommand` on the local
`CopyCatContext`. CopyCat will internally handle routing of commands to the
appropriate node for logging and replication.

```java
public class HttpEndpoint implements Endpoint {
  private final Vertx vertx;
  private CopyCatContext context;
  private HttpServer server;
  private String host;
  private int port;

  public HttpEndpoint() {
    this.vertx = new DefaultVertx();
  }

  @UriInject
  public HttpEndpoint(@UriQueryParam("vertx") Vertx vertx) {
    this.vertx = vertx;
  }

  @UriInject
  public HttpEndpoint(@UriHost String host) {
    this(host, 0);
  }

  @UriInject
  public HttpEndpoint(@UriHost String host, @Optional @UriPort int port) {
    this.host = host;
    this.port = port;
    this.vertx = new DefaultVertx();
  }

  @UriInject
  public HttpEndpoint(@UriHost String host, @Optional @UriPort int port, @UriQueryParam("vertx") Vertx vertx) {
    this.host = host;
    this.port = port;
    this.vertx = vertx;
  }

  @Override
  public void init(CopyCatContext context) {
    this.context = context;
    this.server = vertx.createHttpServer();
    RouteMatcher routeMatcher = new RouteMatcher();
    routeMatcher.post("/:command", new Handler<HttpServerRequest>() {
      @Override
      public void handle(final HttpServerRequest request) {
        request.bodyHandler(new Handler<Buffer>() {
          @Override
          public void handle(Buffer buffer) {
            HttpEndpoint.this.context.submitCommand(request.params().get("command"), new JsonObject(buffer.toString()).toMap(), new AsyncCallback<Object>() {
              @Override
              @SuppressWarnings({"unchecked", "rawtypes"})
              public void call(net.kuujo.copycat.AsyncResult<Object> result) {
                if (result.succeeded()) {
                  request.response().setStatusCode(200);
                  if (result instanceof Map) {
                    request.response().end(new JsonObject().putString("status", "ok").putString("leader", HttpEndpoint.this.context.leader()).putObject("result", new JsonObject((Map) result.value())).encode());                  
                  } else if (result instanceof List) {
                    request.response().end(new JsonObject().putString("status", "ok").putString("leader", HttpEndpoint.this.context.leader()).putArray("result", new JsonArray((List) result.value())).encode());
                  } else {
                    request.response().end(new JsonObject().putString("status", "ok").putString("leader", HttpEndpoint.this.context.leader()).putValue("result", result.value()).encode());
                  }
                } else {
                  request.response().setStatusCode(400);
                }
              }
            });
          }
        });
      }
    });
    server.requestHandler(routeMatcher);
  }

  /**
   * Sets the endpoint host.
   *
   * @param host The TCP host.
   */
  @UriHost
  public void setHost(String host) {
    this.host = host;
  }

  /**
   * Returns the endpoint host.
   *
   * @return The endpoint host.
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the endpoint host, returning the endpoint for method chaining.
   *
   * @param host The TCP host.
   * @return The TCP endpoint.
   */
  public HttpEndpoint withHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Sets the endpoint port.
   *
   * @param port The TCP port.
   */
  @UriPort
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Returns the endpoint port.
   *
   * @return The TCP port.
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the endpoint port, returning the endpoint for method chaining.
   *
   * @param port The TCP port.
   * @return The TCP endpoint.
   */
  public HttpEndpoint withPort(int port) {
    this.port = port;
    return this;
  }

  @Override
  public void start(final AsyncCallback<Void> callback) {
    server.listen(port, host, new Handler<AsyncResult<HttpServer>>() {
      @Override
      public void handle(AsyncResult<HttpServer> result) {
        if (result.failed()) {
          callback.call(new net.kuujo.copycat.AsyncResult<Void>(result.cause()));
        } else {
          callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
        }
      }
    });
  }

  @Override
  public void stop(final AsyncCallback<Void> callback) {
    server.close(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          callback.call(new net.kuujo.copycat.AsyncResult<Void>(result.cause()));
        } else {
          callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
        }
      }
    });
  }

}
```

### Running the key-value store
Finally, we have a complete fault-tolerant in-memory key-value store over
a custom TCP protocol with a user-facing HTTP interface. To tie all this
together, CopyCat provides a helper `CopyCat` class which handles binding
an `Endpoint` to a `CopyCatContext`.

```java
ClusterConfig cluster = new ClusterConfig();
cluster.setLocalMember("tcp://localhost:5005");
cluster.addRemoteMember("tcp://localhost:5006");
cluster.addRemoteMember("tcp://localhost:5007");

CopyCat copycat = new CopyCat("http://locahost:8080", new KeyValueStore(), new FileLog("key-value.log"), cluster);
copycat.start();
```
