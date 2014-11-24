Copycat
=======

### [User Manual](#user-manual)

Copycat is an extensible Java-based implementation of the
[Raft consensus protocol](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

The core of Copycat is a framework designed to integrate with any asynchronous framework
or protocol. Copycat provides a simple extensible API that can be used to build a
strongly consistent, *fault/partition tolerant state machine* over any mode of communication.
Copycat's Raft implementation also supports advanced features of the Raft algorithm such as
snapshotting and dynamic cluster configuration changes and provides *additional optimizations
pipelining, fast Kryo-based serialization, memory-mapped file logging, and read-only state queries.*

Copycat is a pluggable framework, providing protocol and service implementations for
various frameworks such as [Netty](http://netty.io) and [Vert.x](http://vertx.io).

**Please note that Copycat is still undergoing heavy development, and until a beta release,
the API is subject to change.**

This project has been in development for over a year, and it's finally nearing its first release.
Until then, here are a few items on the TODO list:

* ~~Refactor the cluster/protocol APIs to provide more flexibility~~
* ~~Separate commands into separate Command (write) and Query (read) operations~~
* ~~Provide APIs for both synchronous and asynchronous environments~~
* Develop extensive integration tests for configuration, leader election and replication algorithms
* Update documentation for new API changes
* Publish to Maven Central!

Copycat *will* be published to Maven Central once these features are complete. Follow
the project for updates!

*Copycat requires Java 8. Though it has been requested, there are currently no imminent
plans for supporting Java 7. Of course, feel free to fork and PR :-)*

User Manual
===========

**Note: Some of this documentation may be innacurate due to the rapid development currently
taking place on Copycat**

1. [A Brief Introduction](#a-brief-introduction)
1. [Getting started](#getting-started)
   * [Creating a state machine](#creating-a-state-machine)
   * [Providing command types](#providing-command-types)
   * [Taking snapshots](#taking-snapshots)
   * [Configuring the replica](#configuring-the-replica)
   * [Configuring the cluster](#configuring-the-cluster)
   * [Creating the Copycat](#creating-the-copycat)
   * [Setting the log type](#setting-the-log-type)
   * [Submitting commands to the cluster](#submitting-commands-to-the-cluster)
1. [Events](#events)
   * [Start/stop events](#start-stop-events)
   * [Election events](#election-events)
   * [Cluster events](#cluster-events)
   * [State events](#state-events)
1. [Protocols](#protocols-1)
   * [Writing a protocol server](#writing-a-protocol-server)
   * [Writing a protocol client](#writing-a-protocol-client)
   * [The complete protocol](#the-complete-protocol)
   * [Built-in protocols](#built-in-protocols)
      * [Local](#local-protocol)
      * [Netty TCP](#netty-tcp-protocol)
      * [Vert.x Event Bus](#vertx-event-bus-protocol)
      * [Vert.x TCP](#vertx-tcp-protocol)
1. [Services](#services-1)
   * [Using URI annotations with services](#using-uri-annotations-with-services)
   * [Wrapping the Copycat in a service](#wrapping-the-copycat-in-a-service)
   * [Built-in services](#built-in-services)
      * [Vert.x Event Bus](#vertx-event-bus-service)
      * [Vert.x TCP](#vertx-tcp-service)
      * [Vert.x HTTP](#vertx-http-service)
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
   * [Services](#services)

# A brief introduction
Copycat is a "protocol agnostic" implementation of the Raft consensus algorithm. It
provides a framework for constructing a partition-tolerant replicated state machine
over a variety of wire-level protocols. It sounds complicated, but the API is actually
quite simple. Here's a quick example.

```java
public class KeyValueStore implements StateMachine {
  private Map<String, Object> data = new HashMap<>();

  @Query
  public Object get(String key) {
    return data.get(key);
  }

  @Command
  public void set(String key, Object value) {
    data.put(key, value);
  }

  @Command
  public void delete(String key) {
    data.remove(key);
  }

}
```

Here we've created a simple key value store. The state machine is a special object that
exposes `Command` (write) and `Query` (read) operations. Copycat optimizes based on the
operation being performed. By deploying this state machine on several
nodes in a cluster, Copycat will ensure operations (i.e. `get`, `set`, and `delete`) are
applied to the state machine in the order in which they're submitted to the cluster
(log order). Internally, Copycat uses a replicated log to order and replicate commands,
and it uses leader election to coordinate log replication. When the replicated log grows
too large, Copycat will take a snapshot of the state machine state and compact the log.

```java
Log log = new MemoryMappedFileLog("data.log");
```

To configure the Copycat cluster, we simply create a `Cluster` passing in our local and remote endpoints:

```java
Cluster cluster = new Cluster("tcp://localhost:1234", "tcp://localhost:2345", "tcp://localhost:4567");
```

These are the endpoints the nodes within the Copycat cluster use to communicate with one another.
Copycat provides a number of different [protocol](#protocols) and [service](#services)
implementations.

Additionally, Copycat cluster membership is dynamic, and the `Cluster` is `Observable`.
This means that if the `Cluster` is changed, Copycat will pick up the membership change and safely replicate the cluster configuration to other members.

Now that the cluster has been set up, we need a protocol over which it can communicate with other Copycat nodes. Copycat's protocol framework is pluggable, meaning Copycat can communicate over a variety of wire-level protocols. Additionally, Copycat's API is designed to operate in both synchronous and asynchronous environments. In this case, we'll use an asynchronous protocol.

```java
Protocol protocol = new VertxTcpProtocol();
```

Now that the cluster and protocol have been set up, we simply create a `Copycat` instance.

```java
Copycat copycat = new Copycat(new KeyValueStore(), log, cluster, protocol);
```

We can expose the Copycat cluster as an HTTP service by using a `Service`. We'll use
the `VertxHttpService` to expose the cluster via an HTTP server.

```java
Service service = new VertxHttpService(copycat, "localhost", 8080);
service.start();
```

That's it! We've just created a strongly consistent, fault-tolerant key-value store with an
HTTP API in less than 25 lines of code!

```java
public class StronglyConsistentFaultTolerantAndTotallyAwesomeKeyValueStore implements StateMachine {

  public static void main(String[] args) {
    // Create the local file log.
    Log log = new ChronicleLog("data.log");

    // Create the cluster.
    Cluster cluster = new Cluster("tcp://localhost:1234", "tcp://localhost:2345", "tcp://localhost:4567");

    // Create the protocol.
    Protocol protocol = new VertxTcpProtocol();

    // Create a state machine instance.
    StateMachine stateMachine = new StronglyConsistentFaultTolerantAndTotallyAwesomeKeyValueStore();

    // Create a Copycat instance.
    Copycat copycat = new Copycat(stateMachine, log, cluster, protocol);

    // Create an HTTP service and start it.
    VertxHttpService service = new VertxHttpService(copycat, "localhost", 8080);
    service.start();
  }

  private Map<String, Object> data = new HashMap<>();

  @Override
  public byte[] takeSnapshot() {
    return Serializer.getSerializer().serialize(data);
  }

  @Override
  public void installSnapshot(byte[] data) {
    this.data = Serializer.getSerializer().deserialize(data, Map.class);
  }

  @Query
  public Object get(String key) {
    return data.get(key);
  }

  @Command
  public void set(String key, Object value) {
    data.put(key, value);
  }

  @Command
  public void delete(String key) {
    data.remove(key);
  }

}
```

We can now execute commands on the state machine by making `POST` requests to
the HTTP interface.

```
POST http://localhost:8080/set
["foo", "Hello world!"]

200 OK

POST http://localhost:8080/get
["foo"]

200 OK

{
  "result": "Hello world!"
}

POST http://localhost:8080/delete
["foo"]

200 OK
```

Copycat doesn't require that commands be submitted via a service. Rather than
submitting commands via the HTTP service, simply construct a `Copycat`
instance and submit commands directly to the cluster via the copycat.

```java
Copycat copycat = new Copycat(new KeyValueStore(), log, cluster, protocol);

// Set a key in the key-value store.
copycat.submit("set", "foo", "Hello world!").thenRun(() -> {
  copycat.submit("get", "foo").whenComplete((result, error) {
    copycat.submit("delete", "foo").thenRun(() -> System.out.println("Deleted 'foo'"));
  });
});
```

# Getting Started

### Creating a [state machine](#state-machines)
To create a state machine in Copycat, simply implement the `StateMachine`
interface.

```java
public class MyStateMachine implements StateMachine {
}
```

The state machine interface is simply an identifier interface. Arbitrary operations
are added to the state machine via the `@Command` and `@Query` annotations.

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

To annotate a state change method, use the `@Command` annotation.

To annotate a state query method, use the `@Query` annotation.

```java
public class MyStateMachine implements StateMachine {
  private final Map<String, Object> data = new HashMap<>();

  @Query
  public Object read(String key) {
    return data.get(key);
  }

  @Command
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

Users can implement snapshot support in their `StateMachine` implementation by overriding
the default `takeSnapshot` and `installSnapshot` methods.

```java
public class MyStateMachine implements StateMachine {
  private Map<String, Object> data = new HashMap<>();

  @Override
  public byte[] takeSnapshot() {
    return Serializer.getSerializer().serialize(data);
  }

  @Override
  public void installSnapshot(byte[] data) {
    this.data = Serializer.getSerializer().deserialize(data);
  }

}
```

### Configuring the replica
Copycat exposes a configuration API that allows users to configure how Copycat behaves
during elections and how it replicates commands. To configure a Copycat replica, create
a `CopycatConfig` to pass to the `Copycat` constructor.

```java
CopycatConfig config = new CopycatConfig();
```

The `CopycatConfig` exposes the following configuration methods:
* `setElectionTimeout`/`withElectionTimeout` - sets the timeout within which a [follower](#follower)
must receive a `Sync` request from the leader before starting a new election. This timeout
is also used to calculate the election timeout during elections. Defaults to `2000` milliseconds
* `setHeartbeatInterval`/`withHeartbeatInterval` - sets the interval at which the leader will send
heartbeats (`Ping` requests) to followers. Defaults to `500` milliseconds
* `setRequireWriteQuorum`/`withRequireWriteQuorum` - sets whether to require a quorum during write
operations. It is strongly recommended that this remain enabled for consistency. Defaults to `true` (enabled)
* `setWriteQuorumSize`/`withWriteQuorumSize` - sets the absolute write quorum size
* `setWriteQuorumStrategy`/`withWriteQuorumStrategy` - sets a write quorum calculation strategy, an
implementation of `QuorumStrategy`
* `setRequireReadQuorum`/`withRequireReadQuorum` - sets whether to require a quorum during read operations.
Read quorums can optionally be disabled in order to improve performance at the risk of reading stale data.
When read quorums are disabled, the leader will immediately respond to `READ` type commands by applying
the command to its state machine and returning the result. Defaults to `true` (enabled)
* `setReadQuorumSize`/`withReadQuorumSize` - sets the absolute read quorum size
* `setReadQuorumStrategy`/`withReadQuorumStrategy` - sets a read quorum calculation strategy, an
implementation of `QuorumStrategy`
* `setMaxLogSize`/`withMaxLogSize` - sets the maximum log size before a snapshot should be taken. As
entries are appended to the local log, replicas will monitor the size of the local log to determine
whether a snapshot should be taken based on this value. Defaults to `32 * 1024^2`
* `setCorrelationStrategy`/`withCorrelationStrategy` - Copycat generates correlation identifiers
for each request/response pair that it sends. This helps assist in correlating messages across
various protocols. By default, Copycat uses `UuidCorrelationStrategy` - a `UUID` based correlation ID
generator, but depending on the protocol being used, the `MonotonicCorrelationStrategy` which generates
monotonically increasing IDs may be safe to use (it's safe with all core Copycat protocols).
Defaults to a `UUID` based correlation strategy
* `setTimerStrategy`/`withTimerStrategy` - sets the replica timer strategy. This allows users to
control how Copycat's internal timers work. By default, replicas use a `ThreadTimerStrategy`
which uses the Java `Timer` to schedule delays on a background thread. Users can implement their
own `TimerStrategy` in order to, for example, implement an event loop based timer. Timers should
return monotonically increasing timer IDs that never repeat. Note also that timers do not need
to be multi-threaded since Copycat only sets a timeout a couple time a second. Defaults to
a thread-based timer strategy

### Configuring the [cluster](#cluster-configurations)
When a Copycat cluster is first started, its local and remote endpoints must be explicitly provided by the user. However, as the cluster runs, explicit cluster configurations may no longer be required. This is because once a cluster leader has been elected, the leader will replicate its cluster configuration to the rest of the cluster, and Copycat will maintain the cluster's state automatically.

Once the local node has been started, simply adding or removing nodes to the 
`Cluster` may cause the replica's configuration to be updated. However, it's important to remember that as with commands, configuration changes must go through the cluster leader and be replicated to the rest of the cluster. This allows Copycat to ensure that logs remain consistent while nodes are added or removed, but it also means that cluster configuration changes may not be propagated for some period of time after the `Cluster` is updated.

### Creating the Copycat
Copycat replicas are run via a `Copycat`. The Copycat instance is a container
for the replica's `Log`, `StateMachine`, and `Cluster`. To start the replica,
simply call the `start()` method.

```java
Copycat copycat = new Copycat(stateMachine, log, cluster, protocol);
copycat.start();
```

The `start` method returns a `CompletableFuture` which will be completed once the Copycat replica has started.

```java
copycat.start().thenRun(() -> System.out.println("Started!"));
```

### Setting the [log](#logs) type
By default, Copycat uses an in-memory log for simplicity. However, in production
users should use a disk-based log. Copycat provides two `Log` implementations:
* `InMemoryLog` - an in-memory `TreeMap` based log
* `FileLog` - a `RandomAccessFile` based log
* `ChronicleLog` - a fast [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) based log

To set the log to be used by a Copycat replica, simply pass the log instance in
the `Copycat` constructor:

```java
Copycat copycat = new Copycat(new MyStateMachine(), new MemoryMappedFileLog("data.log"), cluster, protocol);
copycat.start();
```

### Submitting [operations](#operations) to the cluster
To submit commands to the Copycat cluster, simply call the `submit` method
on any `Copycat` object.

```java
copycat.submit("get", "foo").thenAccept((result) -> System.out.println(result));
```

The `Copycat` API is supports an arbitrary number of positional arguments. When
a command is submitted, the copycat will return a `CompletableFuture` which will be
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
copycat.on().start().runOnce((event) -> System.out.println("Copycat started"));
```

```java
copycat.events().start().registerHandler((event) -> System.out.println("Copycat started"));
```

```java
copycat.event(Events.START).registerHandler((event) -> System.out.println("Copycat started"));
```

### Election events
Election event handlers are registered on the `ElectionContext` instance.

```java
copycat.on().voteCast().run((event) -> System.out.println(String.format("Vote cast for " + event.candidate())));
```

```java
copycat.event(Events.VOTE_CAST).registerHandler((event) -> {
  System.out.println(String.format("Vote cast for " + event.candidate()));
});
```

copycat.on().leaderElect().run((event) -> System.out.println(String.format("Leader elected")));

```java
copycat.event(Events.LEADER_ELECT).registerHandler((event) -> {
  System.out.println(String.format("%s was elected for term %d", event.leader(), event.term()));
});
```

### Cluster events
Cluster event handlers are registered using the `MEMBERSHIP_CHANGE` constant

```java
copycat.event(Events.STATE_CHANGE).registerHandler((event) -> {
  System.out.println(String.format("%s members", event.members()));
});
```

### State change events
State event handlers are registered using the `STATE_CHANGE` constant

```java
copycat.event(Events.STATE_CHANGE).registerHandler((event) -> {
  System.out.println(String.format("State changed to %s", event.state()));
});
```

# Protocols
Copycat is an abstract API that can implement the Raft consensus algorithm over
any conceivable protocol. To do this, Copycat provides a flexible protocol plugin
system. Protocols use special cluster configurations to configure protocol-specific
settings.

To define a new protocol, simply implement the base `Protocol` interface.
The `Protocol` interface provides the following methods:

```java
public interface Protocol<M extends Member> {

  ProtocolClient createClient(M member);

  ProtocolServer createServer(M member);

}
```

You'll notice that the `Protocol` itself doesn't actually do anything. Instead,
it simply provides factories for `ProtocolClient` and `ProtocolServer`. Additionally,
each protocol is associated with a configurable cluster member type. For instance,
a `TcpProtocol` might be assiciated with a `TcpMember` which contains `host` and
`port` information.

### Writing a protocol server
The `ProtocolServer` interface is implemented by the receiving side of the protocol.
The server's task is quite simple - to call replica callbacks when a message is received.
In order to do so, the `ProtocolServer` provides a methods for registering
a request handler. Protocols and the request handler rely upon `CompletableFuture`
to perform tasks asynchronously.

```java
public interface ProtocolServer {

  void requestHandler(RequestHandler handler);

  CompletableFuture<Void> listen();

  CompletableFuture<Void> close();

}
```

Each `ProtocolServer` should only ever have a single callback for each method registered
at any given time. When the `start` and `stop` methods are called, the server should
obviously start and stop servicing requests respectively. It's up to the server to
transform wire-level messages into the appropriate `Request` instances.

The `RequestHandler` to which the server calls implements the same interface as
`ProtocolClient` below.

### Writing a protocol client
The client's task is equally simple - to send messages to another replica when asked.
In order to do so, the `ProtocolClient` implements the other side of the `ProtocolServer`
callback methods.

```java
public interface ProtocolClient {

  CompletableFuture<PingResponse> ping(PingRequest request);

  CompletableFuture<SyncResponse> sync(SyncRequest request);

  CompletableFuture<PollResponse> poll(PollRequest request);

  CompletableFuture<SubmitResponse> submit(SubmitRequest request);

}
```

## Built-in protocols
Copycat maintains several built-in protocols, some of which are implemented on top
of asynchronous frameworks like [Netty](http://netty.io) and [Vert.x](http://vertx.io).

### Local Protocol
The `local` protocol is a simple protocol that communicates between copycats using
direct method calls. This protocol is intended purely for testing.

```java
Cluster cluster = new Cluster("foo", "bar", "baz");
Copycat copycat = new Copycat(new MyStateMachine(), log, cluster, protocol);
```

### Netty TCP Protocol
The netty `tcp` protocol communicates between replicas using Netty TCP channels.

In order to use Netty protocols, you must add the `copycat-netty` project as a
dependency. Once the `copycat-netty` library is available on your classpath,
Copycat will automatically find the Netty `tcp` protocol.

```java
NettyTcpProtocol protocol = new NettyTcpProtocol();
protocol.setThreads(3);
```

The TCP protocol also provides additional options:

The Netty `tcp` protocol has several additional named options.
* `threads`
* `sendBufferSize`
* `receiveBufferSize`
* `soLinger`
* `trafficClass`
* `acceptBacklog`
* `connectTimeout`

### Vert.x Event Bus Protocol
The Vert.x `EventBus` protocol communicates between replicas on the Vert.x event
bus. The event bus can either be created in a new `Vertx` instance or referenced
in an existing `Vertx` instance.

In order to use Vert.x protocols, you must add the `copycat-vertx` project as a dependency.

*Note that Copycat now provides a prototyped Vert.x 3 event bus protocol*

### Vert.x TCP Protocol
The Vert.x `tcp` protocol communicates between replicas using a simple wire protocol
over Vert.x `NetClient` and `NetServer` instances. To configure the `tcp` protocol,
simply use an ordinary TCP address.

In order to use Vert.x protocols, you must add the `copycat-vertx` project as a dependency.

```java
VertxTcpProtocol protocol = new VertxTcpProtocol();
protocol.setSendBufferSize(1000);
protocol.setReceiveBufferSize(2000);
```

The Vert.x `tcp` protocol has several additional options.
* `sendBufferSize`
* `receiveBufferSize`
* `ssl`
* `keyStorePath`
* `keyStorePassword`
* `trustStorePath`
* `trustStorePassword`
* `acceptBacklog`
* `connectTimeout`

# Services
We've gone through developing custom protocols for communicating between
nodes in Copycat, but a replicated state machine isn't much good if you can't
get much information into it. Nodes need some sort of API that can be exposed to
the outside world. For this, Copycat provides an *services* API that behaves very
similarly to the *protocol* API.

The `Service` interface is very simple:

```java
public interface Service {

  CompletableFuture<Void> start();

  CompletableFuture<Void> stop();

}
```

Services simply wrap a `Copycat` instance and forward requests to the local
copycat via the `Copycat.submit` method.

### Wrapping the Copycat instance in a service
Copycat provides a simple helper class for wrapping a `Copycat` in an
service. To wrap a copycat, use the `Copycat` class.

```java
Cluster cluster = new Cluster("tcp://localhost:1234", "tcp://localhost:2345", "tcp://localhost:4567");
Copycat copycat = new Copycat(new MyStateMachine(), log, cluster, protocol);

Service service = new HttpService(copycat, "localhost", 8080);
service.start();
```

## Built-in services
Copycat also provides a couple of built-in services via the `copycat-vertx`
project. In order to use Vert.x services, you must add the `copycat-vertx`
project as a dependency.

### Vert.x Event Bus Service
The Vert.x event bus service receives submissions over the Vert.x event bus.

```java
Copycat copycat = new Copycat(new MyStateMachine(), log, cluster, protocol);
Service service = new VertxEventBusService(copycat, "some-address");
service.start();
```

### Vert.x TCP Service
The Vert.x TCP service uses a delimited JSON-based protocol:
```java
Copycat copycat = new Copycat(new MyStateMachine(), log, cluster, protocol);
Service service = new VertxTcpService(copycat, "localhost", 5000);
service.start();
```

### Vert.x HTTP Service
The Vert.x HTTP service uses a simple HTTP server which accepts JSON `POST` requests
to the command path. For instance, to submit the `read` command with `{"key":"foo"}`
execute a `POST` request to the `/read` path using a JSON body containing command arguments.

```java
Copycat copycat = new Copycat(new MyStateMachine(), log, cluster, protocol);
Service service = new VertxHttpService(copycat, "localhost", 8080);
service.start();
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

### Services
Copycat provides a framework for building fault-tolerant [state machines](#state-machines) on
the Raft consensus algorithm, but fault-tolerant distributed systems are of no use if they can't
be accessed from the outside world. Copycat's `Service` API facilitates creating user-facing
interfaces (servers) for submitting [commands](#commands) to the Copycat cluster.
