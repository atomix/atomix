Copycat
=======

### [User Manual](#user-manual)

Copycat is an extensible Java-based implementation of the
[Raft consensus protocol](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

The core of Copycat is a framework designed to support a variety of asynchronous frameworks
and protocols. Copycat provides a simple extensible API that can be used to build a
strongly consistent, fault/partition tolerant state machine over any mode of communication.
Copycat's Raft implementation also supports advanced features of the Raft algorithm such as
snapshotting and dynamic cluster configuration changes and provides additional optimizations
for various scenarios such as failure detection and read-only state queries.

Copycat is a pluggable framework, providing protocol and endpoint implementations for
various frameworks such as [Netty](http://netty.io) and [Vert.x](http://vertx.io).

*Note that this version requires Java 8. There is also a [Java 7 compatible version](https://github.com/kuujo/copycat/tree/java-1.7),
however, the Java 7 version may be significantly behind master at any given point*

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
   * [Configuring the replica](#configuring-the-replica)
   * [Configuring the cluster](#configuring-the-cluster)
   * [Creating a dynamic cluster](#creating-a-dynamic-cluster)
   * [Creating the CopyCatContext](#creating-the-copycatcontext)
   * [Setting the log type](#setting-the-log-type)
   * [Submitting commands to the cluster](#submitting-commands-to-the-cluster)
1. [Events](#events)
   * [Start/stop events](#start-stop-events)
   * [Election events](#election-events)
   * [Cluster events](#cluster-events)
   * [State events](#state-events)
1. [Serialization](#serialization)
   * [Providing custom log serializers](#providing-custom-log-serializers)
1. [Protocols](#protocols-1)
   * [Writing a protocol server](#writing-a-protocol-server)
   * [Writing a protocol client](#writing-a-protocol-client)
   * [Injecting URI arguments into a protocol](#injecting-uri-arguments-into-a-protocol)
   * [The complete protocol](#the-complete-protocol)
   * [Built-in protocols](#built-in-protocols)
      * [Local](#local-protocol)
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
Copycat is a "protocol agnostic" implementation of the Raft consensus algorithm. It
provides a framework for constructing a partition-tolerant replicated state machine
over a variety of wire-level protocols. It sounds complicated, but the API is actually
quite simple. Here's a quick example.

```java
public class KeyValueStore extends StateMachine {
  @Stateful
  private Map<String, Object> data = new HashMap<>();

  @Command(type = Command.Type.READ)
  public Object get(String key) {
    return data.get(key);
  }

  @Command(type = Command.Type.WRITE)
  public void set(String key, Object value) {
    data.put(key, value);
  }

  @Command(type = Command.Type.WRITE)
  public void delete(String key) {
    data.remove(key);
  }

}
```

Here we've created a simple key value store. By deploying this state machine on several
nodes in a cluster, Copycat will ensure commands (i.e. `get`, `set`, and `delete`) are
applied to the state machine in the order in which they're submitted to the cluster
(log order). Internally, Copycat uses a replicated log to order and replicate commands,
and it uses leader election to coordinate log replication. When the replicated log grows
too large, Copycat will take a snapshot of the `@Stateful` state machine state and
compact the log.

```java
LogFactory logFactory = new FileLogFactory();
```

To configure the Copycat cluster, we simply create a `ClusterConfig`.

```java
ClusterConfig cluster = new ClusterConfig();
cluster.setLocalMember("tcp://localhost:8080");
cluster.setRemoteMembers("tcp://localhost:8081", "tcp://localhost:8082");
```

Note that the cluster configuration identifies a particular protocol, `tcp`. These are
the endpoints the nodes within the Copycat cluster use to communicate with one another.
Copycat provides a number of different [protocol](#protocols) and [endpoint](#endpoints)
implementations.

Additionally, Copycat cluster membership is dynamic, and the `ClusterConfig` is `Observable`.
This means that if the `ClusterConfig` is changed while the cluster is running, Copycat
will pick up the membership change and replicate the cluster configuration in a safe manner.

Now that the cluster has been set up, we simply create a `CopyCat` instance, specifying
an [endpoint](#endpoints) through which the outside world can communicate with the cluster.

```java
CopyCat copycat = new CopyCat("http://localhost:5000", new KeyValueStore(), logFactory, cluster);
copycat.start();
```

That's it! We've just created a strongly consistent, fault-tolerant key-value store with an
HTTP API in less than 25 lines of code!

```java
public class StronglyConsistentFaultTolerantAndTotallyAwesomeKeyValueStore extends StateMachine {

  public static void main(String[] args) {
    // Create the local file log.
    LogFactory logFactory = new FileLogFactory();

    // Configure the cluster.
    ClusterConfig cluster = new ClusterConfig();
    cluster.setLocalMember("tcp://localhost:8080");
    cluster.setRemoteMembers("tcp://localhost:8081", "tcp://localhost:8082");

    // Create and start a server at localhost:5000.
    new CopyCat("http://localhost:5000", new StronglyConsistentFaultTolerantAndTotallyAwesomeKeyValueStore(), logFactory, cluster).start();
  }

  @Stateful
  private Map<String, Object> data = new HashMap<>();

  @Command(type = Command.Type.READ)
  public Object get(String key) {
    return data.get(key);
  }

  @Command(type = Command.Type.WRITE)
  public void set(String key, Object value) {
    data.put(key, value);
  }

  @Command(type = Command.Type.WRITE)
  public void delete(String key) {
    data.remove(key);
  }

}
```

We can now execute commands on the state machine by making `POST` requests to
the HTTP interface.

```
POST http://localhost:5000/set
["foo", "Hello world!"]

200 OK

POST http://localhost:5000/get
["foo"]

200 OK

{
  "result": "Hello world!"
}

POST http://localhost:5000/delete
["foo"]

200 OK
```

Copycat doesn't require that commands be submitted via an endpoint. Rather than
submitting commands via the HTTP endpoint, simply construct a `CopyCatContext`
instance and submit commands directly to the cluster via the context.

```java
CopyCatContext context = new CopyCatContext(new KeyValueStore(), log, cluster);

// Set a key in the key-value store.
context.submitCommand("set", "foo", "Hello world!").thenRun(() -> {
  context.submitCommand("get", "foo").whenComplete((result, error) {
    context.submitCommand("delete", "foo").thenRun(() -> System.out.println("Deleted 'foo'"));
  });
});
```

# How it works
Copycat uses a Raft-based consensus algorithm to perform leader election and state
replication. Each node in a Copycat cluster may be in one of three states at any
given time - [follower](#followers), [candidate](#candidates), or [leader](#leaders).
Each node in the cluster maintains an internal [log](#logs) of replicated commands.
When a command is submitted to a Copycat cluster, the command is forwarded to the
cluster leader. The leader then logs the command and replicates it to a majority
of the cluster. Once the command has been replicated, it applies the command to its
local state machine and replies with the command result.

*For the most in depth description of how Copycat works, see
[In Search of an Understandable Consensus Algorithm](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf)
by Diego Ongaro and John Ousterhout.*

### State Machines
Each node in a Copycat cluster contains a state machine to which the node applies
[commands](#commands) sent to the cluster. State machines are simply classes that
extend Copycat's `StateMachine` class, but there are a couple of very important
aspects to note about state machines.
* Given the same commands in the same order, state machines should always arrive at
  the same state with the same output.
* Following from that rule, each node's state machine should be identical.

### Commands
Commands are state change instructions that are submitted to the Copycat cluster
and ultimately applied to the state machine. For instance, in a key-value store,
a command could be something like `get` or `set`. Copycat provides unique features
that allow it to optimize handling of certain command types. For instance, read-only
commands which don't contribute to the system state will not be replicated, but commands
that do alter the system state are eventually replicated to all the nodes in the cluster.

### Logs
Copycat nodes share state using a replicated log. When a command is submitted to the
Copycat cluster, the command is appended to the [leader's](#leader-election) log and
replicated to other nodes in the cluster. If a node dies and later restarts, the node
will rebuild its internal state by reapplying logged commands to its state machine.
The log is essential to the operation of the Copycat cluster in that it both helps
maintain consistency of state across nodes and assists in [leader election](#leader-election).
Copycat provides both in-memory logs for testing and file-based logs for production.

### Snapshots
In order to ensure [logs](#logs) do not grow too large for the disk, Copycat replicas periodically
take and persist snapshots of the [state machine](#state-machines) state. Copycat manages snapshots
using a method different than is described in the original Raft paper. Rather than persisting
snapshots to separate snapshot files and replicating snapshots using an additional RCP method,
Copycat appends snapshots directly to the log. This allows Copycat to minimize complexity by
transfering snapshots as a normal part of log replication.

Normally, when a node crashes and recovers, it restores its state using the latest snapshot
and then rebuilds the rest of its state by reapplying committed [command](#commands) entries.
But in some cases, replicas can become so far out of sync that the cluster leader has already
written a new snapshot to its log. In that case, the [leader](#leader-election) will replicate
its latest snapshot to the recovering node, allowing it to start with the leader's snapshot.

### Cluster Configurations
Copycat replicas communicate with one another through a user-defined cluster configuration.
The Copycat cluster is very flexible, and the [protocol](#protocols) underlying the Copycat
cluster is pluggable. Additionally, for cases where cluster configuration may change over
time, Copycat supports runtime cluster configuration changes. As with other state changes,
all cluster configuration changes are performed through the cluster leader.

### Leader Election
Copycat clusters use leader election to maintain synchronization of nodes. In Copycat,
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
to the consistency of Copycat logs. When a replica receives a vote request, it compares
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
It does not implement any specific transport aside from a `local` transport for testing.
Instead, the Copycat API is designed to allow users to implement the transport layer
using the `Protocol` API. Copycat does, however, provide some core protocol implementations
in the `copycat-netty` and `copycat-vertx` projects.

### Endpoints
Copycat provides a framework for building fault-tolerant [state machines](#state-machines) on
the Raft consensus algorithm, but fault-tolerant distributed systems are of no use if they can't
be accessed from the outside world. Copycat's `Endpoint` API facilitates creating user-facing
interfaces (servers) for submitting [commands](#commands) to the Copycat cluster.

# Getting Started

### Creating a [state machine](#state-machines)
To create a state machine in Copycat, simply extend the `StateMachine`
class.

```java
public class MyStateMachine extends StateMachine {
}
```

The state machine interface is simply an identifier interface. *All public methods
within a state machine* can be called as Copycat commands. This is important to remember.

### Providing [command types](#commands)
When a command is submitted to the Copycat cluster, the command is first written
to a [log](#logs) and replicated to other nodes in the cluster. Once the log entry has
been replicated to a majority of the cluster, the command is applied to the leader's
state machine and the response is returned.

In many cases, Copycat can avoid writing any data to the log at all. In particular,
when a read-only command is submitted to the cluster, Copycat does not need to log
and replicate that command since it has no effect on the machine state. Instead, Copycat
can simply ensure that the cluster is in sync, apply the command to the state machine,
and return the result. However, in order for it to do this it needs to have some
additional information about each specific command.

To provide command information to Copycat, use the `@Command` annotation on a command
method.

Each command can have one of three types:
* `READ`
* `WRITE`
* `READ_WRITE`

By default, all commands are of the type `READ_WRITE`. While there is no real difference
between the `WRITE` and `READ_WRITE` types, the `READ` type is important in allowing
Copycat to identify read-only commands. *It is important that any command identified as
a `READ` command never modify the machine state.*

Let's look at an example of a command provider:

```java
public class MyStateMachine extends StateMachine {
  private final Map<String, Object> data = new HashMap<>();

  @Command(type = Command.Type.READ)
  public Object read(String key) {
    return data.get(key);
  }

  @Command(type = Command.Type.WRITE)
  public void write(String key, Object value) {
    data.put(key, value);
  }

}
```

### Taking [snapshots](#snapshots)
One of the issues with a [replicated log](#logs) is that over time it will only continue to grow.
There are a couple of ways to potentially handle this, and Copycat uses the [snapshotting](#snapshots)
method that is recommended by the authors of the Raft algorithm. However, in favor of simplicity,
the Copycat snapshotting implementation does slightly differ from the one described in the Raft
paper. Rather than storing snapshots in a separate snapshot file, Copycat stores snapshots as
normal log entries, making it easier to replicate snapshots when replicas fall too far out
of sync. Additionally, Copycat guarantees that the first entry in any log will always be
a `SnapshotEntry`, again helping to ease the process of replicating snapshots to far
out-of-date replicas.

All snapshot serialization, storage, and loading is handled by Copycat internally. Users need
only annotated stateful fields with the `@Stateful` annotation. Once the log grows to a
predetermined size (configurable in `CopyCatConfig`), Copycat will take a snaphsot of the log
and wipe all previous log entries.

```java
public class MyStateMachine extends StateMachine {
  @Stateful
  private Map<String, Object> data = new HashMap<>();
}
```

### Configuring the replica
Copycat exposes a configuration API that allows users to configure how Copycat behaves
during elections and how it replicates commands. To configure a Copycat replica, create
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
* `setCorrelationStrategy`/`withCorrelationStrategy` - Copycat generates correlation identifiers
for each request/response pair that it sends. This helps assist in correlating messages across
various protocols. By default, Copycat uses `UuidCorrelationStrategy` - a `UUID` based correlation ID
generator, but depending on the protocol being used, the `MonotonicCorrelationStrategy` which generates
monotonically increasing IDs may be safe to use (it's safe with all core Copycat protocols).
Defaults to `UuidCorrelationStrategy`
* `setTimerStrategy`/`withTimerStrategy` - sets the replica timer strategy. This allows users to
control how Copycat's internal timers work. By default, replicas use a `ThreadTimerStrategy`
which uses the Java `Timer` to schedule delays on a background thread. Users can implement their
own `TimerStrategy` in order to, for example, implement an event loop based timer. Timers should
return monotonically increasing timer IDs that never repeat. Note also that timers do not need
to be multi-threaded since Copycat only sets a timeout a couple time a second. Defaults to
`ThreadTimerStrategy`

### Configuring the [cluster](#cluster-configurations)
When a Copycat cluster is first started, the [cluster configuration](#cluster-configurations)
must be explicitly provided by the user. However, as the cluster runs, explicit cluster
configurations may no longer be required. This is because once a cluster leader has been
elected, the leader will replicate its cluster configuration to the rest of the cluster,
and the user-defined configuration will be replaced by an internal configuration.

To configure the Copycat cluster, create a `ClusterConfig`.

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
[The protocol used by Copycat is pluggable](#protocols), so member address formats may
differ depending on the protocol you're using.

To set remote cluster members, use the `setRemoteMembers` or `addRemoteMember` method:

```java
cluster.addRemoteMember("tcp://localhost:1235");
cluster.addRemoteMember("tcp://localhost1236");
```

### Creating a [dynamic cluster](#cluster-configurations)
The Copycat cluster configuration is `Observable`, and once the local node is elected
leader, it will begin observing the configuration for changes.

Once the local node has been started, simply adding or removing nodes from the observable
`ClusterConfig` may cause the replica's configuration to be updated. However,
it's important to remember that as with commands, configuration changes must go through
the cluster leader and be replicated to the rest of the cluster. This allows Copycat
to ensure that logs remain consistent while nodes are added or removed, but it also
means that cluster configuration changes may not be propagated for some period of
time after the `ClusterConfig` is updated.

### Creating the CopyCatContext
Copycat replicas are run via a `CopyCatContext`. The Copycat context is a container
for the replica's `Log`, `StateMachine`, and `ClusterConfig`. To start the replica,
simply call the `start()` method.

```java
CopyCatContext context = new CopyCatContext(stateMachine, cluster);
context.start();
```

### Setting the [log](#logs) type
By default, Copycat uses an in-memory log for simplicity. However, in production
users should use a disk-based log. Copycat provides two `Log` implementations:
* `MemoryLogFactory` - an in-memory `TreeMap` based log factory
* `FileLogFactory` - a `RandomAccessFile` based log factory

To set the log to be used by a Copycat replica, simply pass the log instance in
the `CopyCatContext` constructor:

```java
CopyCatContext context = new CopyCatContext(new MyStateMachine(), new FileLogFactory(), cluster);
context.start();
```

### Submitting [commands](#commands) to the cluster
To submit commands to the Copycat cluster, simply call the `submitCommand` method
on any `CopyCatContext`.

```java
context.submitCommand("get", "foo").thenAccept((result) -> System.out.println(result));
```

The `CopyCatContext` API is supports an arbitrary number of positional arguments. When
a command is submitted, the context will return a `CompletableFuture` which will be
completed once the command result is received. The command will automatically
be forwarded on to the current cluster [leader](#leaders). If the cluster does not have any
currently [elected](#leader-election) leader (or the node to which the command is submitted
doesn't know of any cluster leader) then the submission will fail.

## Events
Copycat provides an API that allows users to listen for various events that occur throughout
the lifetime of a Copycat cluster. To subscribe to an event, register an `EventHandler` on
any `EventHandlerRegistry` or `EventContext`. Events can be registered for elections, state changes,
and cluster membership changes.

### Start/stop events
Start/stop event handlers are registered using the `start()` and `stop()` event methods:

```java
context.on().start().runOnce((event) -> System.out.println("Context started"));
```

```java
context.events().start().registerHandler((event) -> System.out.println("Context started"));
```

```java
context.event(Events.START).registerHandler((event) -> System.out.println("Context started"));
```

### Election events
Election event handlers are registered on the `ElectionContext` instance.

```java
context.on().voteCast().run((event) -> System.out.println(String.format("Vote cast for " + event.candidate())));
```

```java
context.event(Events.VOTE_CAST).registerHandler((event) -> {
  System.out.println(String.format("Vote cast for " + event.candidate()));
});
```

context.on().leaderElect().run((event) -> System.out.println(String.format("Leader elected")));

```java
context.event(Events.LEADER_ELECT).registerHandler((event) -> {
  System.out.println(String.format("%s was elected for term %d", event.leader(), event.term()));
});
```

### Cluster events
Cluster event handlers are registered using the `MEMBERSHIP_CHANGE` constant

```java
context.event(Events.STATE_CHANGE).registerHandler((event) -> {
  System.out.println(String.format("%s members", event.members()));
});
```

### State change events
State event handlers are registered using the `STATE_CHANGE` constant

```java
context.event(Events.STATE_CHANGE).registerHandler((event) -> {
  System.out.println(String.format("State changed to %s", event.state()));
});
```

# Serialization
Copycat provides a pluggable serialization API that allows users to control how log
entries are serialized to the log. Copycat provides two serializer implementations, one
using the core Java serialization mechanism, and one [Jackson](http://jackson.codehaus.org/)
based serializer (the default). To configure the serializer in use, add a file to the classpath
at `META-INF/services/net/kuujo/copycat/Serializer` containing the serializer factory class name:
* `net.kuujo.copycat.setializer.impl.JacksonSerializerFactory` (default)
* `net.kuujo.copycat.serializer.impl.JavaSerializerFactory`

### Providing custom log serializers
Copycat locates log serializers via its custom service loader implementation. To provide
a custom serializer to Copycat, simply add a configuration file to your classpath at
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
This is the basic default serializer used by Copycat:

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
Copycat is an abstract API that can implement the Raft consensus algorithm over
any conceivable protocol. To do this, Copycat provides a flexible protocol plugin
system. Protocols use special URIs - such as `local:foo` or `tcp://localhost:5050` -
and Copycat uses a custom service loader similar to the Java service loader. Using
URIs, a protocol can be constructed and started by Copycat without the large amounts
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

  CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request);

  CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);

  CompletableFuture<SubmitCommandResponse> submitCommand(SubmitCommandRequest request);

}
```

### Injecting URI arguments into a protocol
You may be wondering how you can get URI arguments into your protocol implementation.
Copycat provides special annotations that can be used to inject specific URI parts
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

URI arguments are injected into protocol instances via bean properties or annotated
fields. Let's take a look at an example of setter injection:

```java
public class HttpProtocol implements Protocol {
  private String host;

  @UriHost
  public void setHost(String host) {
    this.host = host;
  }

  public String getHost() {
    return host;
  }

}
```

By default, if a bean property is not annotated with any URI annotation,
the URI injector will attempt to inject a named query parameter into the setter.

```java
public class HttpProtocol implements Protocol {
  private String host;

  public void setHost(String host) {
    this.host = host;
  }

  public String getHost() {
    return host;
  }

}
```

Obviously URI parameters are limiting in type. The Copycat URI injector supports
complex types via a `Registry` instance. Registry lookups can be performed by
prefixing `@UriQueryParam` names with the `$` prefix.

```java
Registry registry = new BasicRegistry();
registry.bind("http_client", new HttpClient("localhost", 8080));
String uri = "http://copycat?client=$http_client";
```

The URI above can be used to inject an `HttpClient` into the `HttpProtocol` via
the `Registry`.

```java
public class HttpProtocol implements Protocol {
  private HttpClient client;

  public void setClient(HttpClient client) {
    this.client = client;
  }

  public HttpClient getClient() {
    return client;
  }

}
```

The URI injector can also inject annotated fields directly.

```java
public class HttpProtocol implements Protocol {
  @UriHost private String host;
  @UriPort private int port;
}
```

### Injecting the CopyCatContext
The CopyCatContext is automatically injected into any `Protocol` implementation.
When URI injection occurs, the URI injector searches fields for a `CopyCatContext`
type field and automatically injects the context into that field.

```java
public class HttpProtocol implements Protocol {
  private CopyCatContext context;
}
```

### The complete protocol
Now that we have all that out of the way, here's the complete `Protocol` implementation:

```java
public class HttpProtocol implements Protocol {
  private String host;
  private int port;
  private String path;

  public HttpProtocol() {
  }

  @UriHost
  public void setHost(String host) {
    this.host = host;
  }

  public String getHost() {
    return host;
  }

  @UriPort
  public void setPort(int port) {
    this.port = port;
  }

  @UriPath
  public void setPath(String path) {
    this.path = path;
  }

  public String getPath() {
    return path;
  }

  @Override
  public ProtocolClient createClient() {
    return new HttpProtocolClient(new HttpClient(host, port), path);
  }

  @Override
  public ProtocolServer createServer() {
    return new HttpProtocolServer(new HttpServer(host, port), path);
  }

}
```

### Configuring the cluster with custom protocols

Let's take a look at an example of how to configure the Copycat cluster when using
custom protocols.

```java
ClusterConfig cluster = new ClusterConfig("http://localhost:8080/copycat");
cluster.addRemoteMember("http://localhost:8081/copycat");
cluster.addRemoteMember("http://localhost:8082/copycat");
```

Note that Copycat does not particularly care about the protocol of any given node
in the cluster. Theoretically, different nodes could be connected together by any
protocol they want (though I can't imagine why one would want to do such a thing).
For the local replica, the protocol's server is used to receive messages. For remote
replicas, each protocol instance's client is used to send messages to those replicas.

## Built-in protocols
Copycat maintains several built-in protocols, some of which are implemented on top
of asynchronous frameworks like [Netty](http://netty.io) and [Vert.x](http://vertx.io).

### Local Protocol
The `local` protocol is a simple protocol that communicates between contexts using
direct method calls. This protocol is intended purely for testing.

```java
Registry registry = new ConcurrentRegistry();
ClusterConfig cluster = new ClusterConfig("local:foo");
cluster.setRemoteMembers("local:bar", "local:baz");
CopyCatContext context = new CopyCatContext(new MyStateMachine, cluster, registry);
```

Note that you should use a `ConcurrentRegistry` when using the `local` protocol.

### Netty TCP Protocol
The netty `tcp` protocol communicates between replicas using Netty TCP channels.

In order to use Netty protocols, you must add the `copycat-netty` project as a
dependency. Once the `copycat-netty` library is available on your classpath,
Copycat will automatically find the Netty `tcp` protocol.

```java
ClusterConfig cluster = new ClusterConfig("tcp://localhost:1234");
```

Additional named arguments can be passed as query arguments:

```
tcp://localhost:1234?sendBufferSize=5000&receiveBufferSize=5000
```

The Netty `tcp` protocol has several additional named options.
* `threads`
* `sendBufferSize`
* `receiveBufferSize`
* `soLinger`
* `trafficClass`
* `acceptBacklog`
* `connectTimeout`

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

Additional named arguments can be passed as query arguments:

```
tcp://localhost:1234?sendBufferSize=5000&receiveBufferSize=5000
```

The Vert.x `tcp` protocol has several additional named options.
* `sendBufferSize`
* `receiveBufferSize`
* `ssl`
* `keyStorePath`
* `keyStorePassword`
* `trustStorePath`
* `trustStorePassword`
* `acceptBacklog`
* `connectTimeout`

# Endpoints
We've gone through developing custom protocols for communicating between
nodes in Copycat, but a replicated state machine isn't much good if you can't
get much information into it. Nodes need some sort of API that can be exposed to
the outside world. For this, Copycat provides an *endpoints* API that behaves very
similarly to the *protocol* API.

To register an endpoint, simply add a file to the `META-INF/services/net/kuujo/copycat/endpoints`
directory, using the endpoint name as the file name. The file should contain a string referencing
a class that implements the `Endpoint` interface.

The `Endpoint` interface is very simple:

```java
public interface Endpoint {

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
Copycat provides a simple helper class for wrapping a `CopyCatContext in an
endpoint. To wrap a context, use the `CopyCat` class. The `CopyCat` constructor
simply accepts a single additional argument which is the endpoint URI.

```java
ClusterConfig cluster = new ClusterConfig("tcp://localhost:5555", "tcp://localhost:5556", "tcp://localhost:5557");
CopyCat copycat = new CopyCat("http://localhost:8080", new MyStateMachine(), cluster)
copycat.start();
```

## Built-in endpoints
Copycat also provides a couple of built-in endpoints via the `copycat-vertx`
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
