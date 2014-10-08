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
pipelining, fast serialization, and read-only state queries.

Copycat is a pluggable framework, providing protocol and service implementations for
various frameworks such as [Netty](http://netty.io) and [Vert.x](http://vertx.io).

**Please note that Copycat is still undergoing heavy development, and until a beta release,
the API is subject to change.**

* The cluster/protocol APIs were recently refactor to provide more flexibility
* Work on extensive integration tests is currently under way
* Work to provide state machine proxies is next up on the TODO list
* A full featured fault-tolerant cluster manager/service is being developed as well

*Copycat requires Java 8. Though it has been requested, there are currently no imminent
plans for supporting Java 7. Of course, feel free to fork and PR :-)*

User Manual
===========

1. [A Brief Introduction](#a-brief-introduction)
1. [Getting started](#getting-started)
   * [Creating a state machine](#creating-a-state-machine)
   * [Providing command types](#providing-command-types)
   * [Taking snapshots](#taking-snapshots)
   * [Configuring the replica](#configuring-the-replica)
   * [Configuring the cluster](#configuring-the-cluster)
   * [Creating a dynamic cluster](#creating-a-dynamic-cluster)
   * [Creating the CopycatContext](#creating-the-copycatcontext)
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
   * [Wrapping the CopycatContext in a service](#wrapping-the-copycatcontext-in-a-service)
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
Log log = new MemoryMappedFileLog("data.log");
```

To configure the Copycat cluster, we simply create a `ClusterConfig` from which
we can create a `Cluster`.

```java
TcpClusterConfig config = new TcpClusterConfig();
config.setLocalMember(new TcpMember("localhost", 1234));
config.setRemoteMembers(new TcpMember("localhost", 2345), new TcpMember("localhost", 4567));

TcpCluster cluster = new TcpCluster(config);
```

Note that the cluster configuration identifies a particular protocol, `tcp`. These are
the endpoints the nodes within the Copycat cluster use to communicate with one another.
Copycat provides a number of different [protocol](#protocols) and [service](#services)
implementations.

Additionally, Copycat cluster membership is dynamic, and the `Cluster` is `Observable`.
This means that if the `ClusterConfig` is changed while the cluster is running, Copycat
will pick up the membership change and replicate the cluster configuration in a safe manner.

Now that the cluster has been set up, we need a service through which remote users can
access the service. For this, Copycat provides the `Service` type. Similar to protocols,
services are implementations of a protocol such as TCP or HTTP, but they're intended
to serve as the outside facing interface of the Copycat cluster.

```java
HttpService service = new HttpService("localhost", 8080);
```

Now that the cluster has been set up, we simply create a `CopyCat` instance, specifying
an [service](#services) through which the outside world can communicate with the cluster.

```java
Copycat copycat = Copycat.copycat(service, new KeyValueStore(), log, cluster);
copycat.start();
```

That's it! We've just created a strongly consistent, fault-tolerant key-value store with an
HTTP API in less than 25 lines of code!

```java
public class StronglyConsistentFaultTolerantAndTotallyAwesomeKeyValueStore extends StateMachine {

  public static void main(String[] args) {
    // Create the local file log.
    Log log = new MemoryMappedFileLog("data.log");

    // Configure the cluster.
    TcpClusterConfig config = new TcpClusterConfig();
    config.setLocalMember(new TcpMember("localhost", 1234));
    config.setRemoteMembers(new TcpMember("localhost", 2345), new TcpMember("localhost", 4567));

    // Create the cluster.
    TcpCluster cluster = new TcpCluster(config);

    // Create an HTTP service.
    HttpService service = new HttpService("localhost", 8080);

    // Create a Copycat instance and start it.
    Copycat copycat = Copycat.copycat(service, log, cluster);
    copycat.start();
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

Copycat doesn't require that commands be submitted via a service. Rather than
submitting commands via the HTTP service, simply construct a `CopycatContext`
instance and submit commands directly to the cluster via the context.

```java
CopycatContext context = CopycatContext.context(new KeyValueStore(), log, cluster);

// Set a key in the key-value store.
context.submitCommand("set", "foo", "Hello world!").thenRun(() -> {
  context.submitCommand("get", "foo").whenComplete((result, error) {
    context.submitCommand("delete", "foo").thenRun(() -> System.out.println("Deleted 'foo'"));
  });
});
```

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

Users can implement snapshot support in their `StateMachine` implementation by overriding
the default `takeSnapshot` and `installSnapshot` methods.

```java
public class MyStateMachine extends StateMachine {
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
a `CopycatConfig` to pass to the `CopycatContext` constructor.

```java
CopycatConfig config = new CopycatConfig();
```

The `CopycatConfig` exposes the following configuration methods:
* `setElectionTimeout`/`withElectionTimeout` - sets the timeout within which a [follower](#follower)
must receive an `AppendEntries` request from the leader before starting a new election. This timeout
is also used to calculate the election timeout during elections. Defaults to `2000` milliseconds
* `setHeartbeatInterval`/`withHeartbeatInterval` - sets the interval at which the leader will send
heartbeats (`AppendEntries` requests) to followers. Defaults to `500` milliseconds
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
When a Copycat cluster is first started, the [cluster configuration](#cluster-configurations)
must be explicitly provided by the user. However, as the cluster runs, explicit cluster
configurations may no longer be required. This is because once a cluster leader has been
elected, the leader will replicate its cluster configuration to the rest of the cluster,
and the user-defined configuration will be replaced by an internal configuration.

To configure the Copycat cluster, create a `ClusterConfig` of the desired cluster type.

```java
ClusterConfig<Member> config = new ClusterConfig<>();
```

Each cluster configuration must contain a *local* member and a set of *remote* members.
To define the *local* member, pass the member address to the cluster configuration
constructor or call the `setLocalMember` method.

```java
config.setLocalMember(new Member("foo"));
```

Note that the configuration uses URIs to identify nodes.
[The protocol used by Copycat is pluggable](#protocols), so member address formats may
differ depending on the protocol you're using.

To set remote cluster members, use the `setRemoteMembers` or `addRemoteMember` method:

```java
config.addRemoteMember(new Member("bar"));
config.addRemoteMember(new Member("baz"));
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

### Creating the CopycatContext
Copycat replicas are run via a `CopycatContext`. The Copycat context is a container
for the replica's `Log`, `StateMachine`, and `ClusterConfig`. To start the replica,
simply call the `start()` method.

```java
CopycatContext context = CopycatContext.context(stateMachine, log, cluster);
context.start();
```

### Setting the [log](#logs) type
By default, Copycat uses an in-memory log for simplicity. However, in production
users should use a disk-based log. Copycat provides two `Log` implementations:
* `InMemoryLog` - an in-memory `TreeMap` based log
* `FileLog` - a `RandomAccessFile` based log
* `MemoryMappedFileLog` - a fast [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) based log

To set the log to be used by a Copycat replica, simply pass the log instance in
the `CopycatContext` constructor:

```java
CopycatContext context = CopycatContext.context(new MyStateMachine(), new MemoryMappedFileLog("data.log"), cluster);
context.start();
```

### Submitting [commands](#commands) to the cluster
To submit commands to the Copycat cluster, simply call the `submitCommand` method
on any `CopycatContext`.

```java
context.submitCommand("get", "foo").thenAccept((result) -> System.out.println(result));
```

The `CopycatContext` API is supports an arbitrary number of positional arguments. When
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
In order to do so, the `ProtocolServer` provides a number of methods for registering
callbacks for each command. Each callback is in the form of an `AsyncCallback` instance.

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

  CompletableFuture<PollResponse> requestVote(PollRequest request);

  CompletableFuture<SubmitResponse> submitCommand(SubmitRequest request);

}
```

### Configuring the cluster with custom protocols

Custom protocols should usually implement a custom extension of `Cluster` and
`ClusterConfig` as well. For instance, `TcpProtocol` also implements `TcpCluster`
and `TcpClusterConfig` for configuring the TCP cluster with `TcpMember` instances.

```java
TcpClusterConfig config = new TcpClusterConfig();
config.setLocalMember(new TcpMember(new TcpMemberConfig().setHost("localhost").setPort(1234)));
config.addRemoteMember(new TcpMember(new TcpMemberConfig().setHost("localhost").setPort(2345)));
config.addRemoteMember(new TcpMember(new TcpMemberConfig().setHost("localhost").setPort(3456)));

TcpCluster cluster = new TcpCluster(config);
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
LocalClusterConfig config = new LocalClusterConfig()
  .withLocalMember(new Member("foo"))
  .withRemoteMembers(new Member("bar"), new Member("baz"));
LocalCluster cluster = new LocalCluster(config);

CopycatContext context = CopycatContext.context(new MyStateMachine(), log, cluster);
```

Note that you should use a `ConcurrentRegistry` when using the `local` protocol.

### Netty TCP Protocol
The netty `tcp` protocol communicates between replicas using Netty TCP channels.

In order to use Netty protocols, you must add the `copycat-netty` project as a
dependency. Once the `copycat-netty` library is available on your classpath,
Copycat will automatically find the Netty `tcp` protocol.

```java
TcpClusterConfig config = new TcpClusterConfig();
config.setLocalMember(new TcpMember(new TcpMemberConfig().setHost("localhost").setPort(1234)));
config.addRemoteMember(new TcpMember(new TcpMemberConfig().setHost("localhost").setPort(2345)));
config.addRemoteMember(new TcpMember(new TcpMemberConfig().setHost("localhost").setPort(3456)));

TcpCluster cluster = new TcpCluster(config);
cluster.protocol().setThreads(3);
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
The Vert.x `eventbus` protocol communicates between replicas on the Vert.x event
bus. The event bus can either be created in a new `Vertx` instance or referenced
in an existing `Vertx` instance.

In order to use Vert.x protocols, you must add the `copycat-vertx` project as a dependency.

```java
EventBusClusterConfig config = new EventBusClusterConfig();
config.setLocalMember(new EventBusMember("foo"));
config.addRemoteMember(new EventBusMember("bar"));
config.addRemoteMember(new EventBusMember("baz"));

EventBusCluster cluster = new EventBusCluster(config);
```

*Note that Copycat now provides a prototyped Vert.x 3 event bus protocol*

### Vert.x TCP Protocol
The Vert.x `tcp` protocol communicates between replicas using a simple wire protocol
over Vert.x `NetClient` and `NetServer` instances. To configure the `tcp` protocol,
simply use an ordinary TCP address.

In order to use Vert.x protocols, you must add the `copycat-vertx` project as a dependency.

```java
TcpClusterConfig config = new TcpClusterConfig();
config.setLocalMember(new TcpMember(new TcpMemberConfig().setHost("localhost").setPort(1234)));
config.addRemoteMember(new TcpMember(new TcpMemberConfig().setHost("localhost").setPort(2345)));
config.addRemoteMember(new TcpMember(new TcpMemberConfig().setHost("localhost").setPort(3456)));

TcpCluster cluster = new TcpCluster(config);

cluster.protocol().setSendBufferSize(1000);
cluster.protocol().setReceiveBufferSize(2000);
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

Services simply wrap the `CopycatContext` and forward requests to the local
context via the `CopycatContext.submitCommand` method.

### Using URI annotations with services
Services support all the same URI annotations as do protocols. See the protocol
[documentation on URI annotations](#injecting-uri-arguments-into-a-protocol)
for a tutorial on injecting arguments into custom services.

### Wrapping the CopycatContext in a service
Copycat provides a simple helper class for wrapping a `CopycatContext in an
service. To wrap a context, use the `Copycat` class.

```java
TcpClusterConfig config = new TcpClusterConfig();
config.setLocalMember(new TcpMember("localhost", 1234));
config.setRemoteMembers(new TcpMember("localhost", 2345), new TcpMember("localhost", 3456));
TcpCluster cluster = new TcpCluster(config);

Copycat copycat = Copycat.copycat(new HttpService("localhost", 8080), new MyStateMachine(), cluster)
copycat.start();
```

## Built-in services
Copycat also provides a couple of built-in services via the `copycat-vertx`
project. In order to use Vert.x services, you must add the `copycat-vertx`
project as a dependency.

### Vert.x Event Bus Service
The Vert.x event bus service receives submissions over the Vert.x event bus.

```java
Copycat copycat = Copycat.copycat(new EventBusService("some-address"), new MyStateMachine(), cluster);
```

### Vert.x TCP Service
The Vert.x TCP service uses a delimited JSON-based protocol:
```java
Copycat copycat = Copycat.copycat(new TcpService("localhost", 8080), new MyStateMachine(), cluster);
```

### Vert.x HTTP Service
The Vert.x HTTP service uses a simple HTTP server which accepts JSON `POST` requests
to the command path. For instance, to submit the `read` command with `{"key":"foo"}`
execute a `POST` request to the `/read` path using a JSON body containing command arguments.

```java
Copycat copycat = Copycat.copycat(new HttpService("localhost", 8080), new MyStateMachine(), cluster);
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
