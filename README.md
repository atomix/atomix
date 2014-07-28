CopyCat
=======
CopyCat is a fault-tolerant state machine replication framework built on the
Raft consensus algorithm as described in
[the excellent paper by Diego Ongaro and John Ousterhout](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

The core of CopyCat is a framework designed to support a variety of protocols and
transports. CopyCat provides a simple extensible API that can be used to build a
fault-tolerant state machine over TCP, HTTP/REST, messaging systems, or any other
form of communication. The CopyCat Raft implementation supports advanced features
of the Raft algorithm such as snapshotting and dynamic cluster configuration changes.

## Table of Contents

1. [How it works](#how-it-works)
   * [State machines](#state-machines)
   * [Logs](#logs)
   * [Snapshots](#snapshots)
   * [Cluster configurations](#cluster-configurations)
   * [Leader election](#leader-election)
   * [Followers](#followers)
   * [Candidates](#candidates)
   * [Leaders](#leaders)
1. [Getting started](#getting-started)
   * [Creating a state machine](#creating-a-state-machine)
   * [Providing command types](#providing-command-types)
   * [Snapshotting](#snapshotting)
   * [Creating an annotated state machine](#creating-an-annotated-state-machine)
   * [Configuring the cluster](#configuring-the-cluster)
   * [Creating a dynamic cluster](#creating-a-dynamic-cluster)
   * [Creating the CopyCatContext](#creating-the-copycatcontext)
   * [Setting the log type](#setting-the-log-type)
   * [Submitting commands to the cluster](#submitting-commands-to-the-cluster)
1. [Protocols](#protocols)
   * [Writing a protocol server](#writing-a-protocol-server)
   * [Writing a protocol client](#writing-a-protocol-client)
   * [Injecting URI arguments into a protocol](#injecting-uri-arguments-into-a-protocol)
   * [Using multiple URI annotations on a single parameter](#using-multiple-uri-annotations-on-a-single-parameter)
   * [Making annotated URI parameters optional](#making-annotated-uri-parameters-optional)
   * [The complete protocol](#the-complete-protocol)
   * [Built-in protocols](#built-in-protocols)
      * [Direct](#direct-protocol)
      * [Vert.x Event Bus](#vertx-event-bus-protocol)
      * [Vert.x TCP](#vertx-tcp-protocol)
1. [Endpoints](#endpoints)
   * [Using URI annotations with endpoints](#using-uri-annotations-with-endpoints)
   * [Wrapping the CopyCatContext in an endpoint service](#wrapping-the-copycatcontext-in-an-endpoint-service)
   * [Built-in endpoints](#build-in-endpoints)
      * [Vert.x Event Bus](#vertx-event-bus-endpoint)
      * [Vert.x TCP](#vertx-tcp-endpoint)
      * [Vert.x HTTP](#vertx-http-endpoint)

## How it works
CopyCat uses a Raft-based consensus algorithm to perform leader election and state
replication. Each node in a CopyCat cluster may be in one of three states at any
given time - [follower](#followers), [candidate](#candidates), or [leader](#leaders).
Each node in the cluster maintains an internal [log](#logs) of replicated commands.
When a command is submitted to a CopyCat cluster, the command is forwarded to the
cluster leader. The leader then logs the command and replicates it to a majority
of the cluster. Once the command has been replicated, it applies the command to its
local state machine and replies with the command result.

### State Machines
Each node in a CopyCat cluster contains a state machine to which the node applies
commands sent to the cluster. State machines are simply classes that implement
CopyCat's `StateMachine` interface, but there are a couple of very important aspects
to note about state machines.
* Given the same commands in the same order, state machines should always arrive at
  the same state with the same output.
* Following from that rule, each node's state machine should be identical.

### Logs
CopyCat replicates state using an internal log. When a command is submitted to the
CopyCat cluster, the command will be appended to the log and replicated to other
nodes in the cluster. The log is essential to the operation of the CopyCat cluster
in that it both helps maintain consistency of state across nodes and assists in
leader election.

### Snapshots
In order to ensure logs do not grow too large for the disk, CopyCat replicas periodically
take and persist snapshots of the state machine state. In CopyCat, when a snapshot is
taken, the snapshot is appended to the local log, and all committed entries are subsequently
removed from the log.

Normally, when a node crashes and recovers, it restores its state using the latest snapshot
and then rebuilds the rest of its state by reapplying committed log entries. But in some
cases, replicas can become so far out of sync that the cluster leader has already written
a new snapshot to its log. In that case, the leader will replicate its latest snapshot
to the recovering node, allowing it to start with the leader's snapshot.

### Cluster configurations
CopyCat replicas communicate with one another through a user-defined cluster configuration.
The CopyCat cluster is very flexible, and the [protocol](#protocols) underlying the CopyCat
cluster is pluggable. Additionally, for cases where cluster configuration may change over
time, CopyCat supports runtime cluster configuration changes. As with other state changes,
all cluster configuration changes are performed through the cluster leader.

### Leader election
CopyCat clusters use leader election to maintain synchronization of nodes. In CopyCat,
all state changes are performed via the leader. This leads to a much simpler implementation
of single-copy consistency.

### Followers
When a node is first started, it is initialized to the follower state. The follower
serves only to listen for synchronization requests from cluster leaders and apply
replicated log entries to its state machine. If the follower does not receive a
request from the cluster leader for a configurable amount of time, it will transition
to the candidate state and start a new election.

### Candidates
The candidate state occurs when a replica is announcing its candidacy to become the
cluster leader. Candidacy occurs when the replica has not heard from the cluster leader
for a configurable amount of time. When a replica becomes a candidate, it requests
votes from each other member of the cluster. The voting algorithm is essential to
the consistency of CopyCat logs. When a replica receives a vote request, it compares
the status of the requesting node's log to its own log and decides whether to vote
for the candidate based on how up-to-date the candidates log is. This ensures that
only replicas with the most up-to-date logs can become the cluster leader.

### Leaders
Once a replica has won an election, it transitions to the leader state. The leader
is the node through which all commands and configuration changes are performed.
When a command is submitted to the cluster, the command is forwarded to the leader.
Based on the command type - read or write - the leader will then replicate the command
to its followers, apply it to its state machine, and return the result. The leader
is also responsible for maintaining log consistency during
[cluster configuration changes](#cluster-configurations).

## Getting Started

### Creating a state machine
To create a state machine in CopyCat, simply implement the `StateMachine`
interface. The state machine interface exposes three methods:

```java
public interface StateMachine {

  Map<String, Object> createSnapshot();

  void installSnapshot(Map<String, Object> snapshot);

  Map<String, Object> applyCommand(String command, Map<String, Object> args);

}
```

The first two methods are for snapshot support, but more on that later. The most
important method in the state machine is the `applyCommand` method. What's important
to remember when writing a state machine is: the machine should always arrive at
the same state and provide the same output given the same commands in the same order.
This means your state machine should not rely on mutable data sources such as databases.

## Providing command types
When a command is submitted to the CopyCat cluster, the command is first written
to a log and replicated to other nodes in the cluster. Once the log entry has been
replicated to a majority of the cluster, the command is applied to the leader's
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
  private static final CommandInfo READ = new GenericCommandInfo("read", CommandInfo.Type.READ);
  private static final CommandInfo WRITE = new GenericCommandInfo("write", CommandInfo.Type.WRITE);
  private static final CommandInfo NONE = new GenericCommandInfo("none", CommandInfo.Type.READ_WRITE);

  @Override
  public CommandInfo getCommandInfo(String command) {
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

### Snapshotting
One of the issues with a replicated log is that over time it will only continue to grow.
There are a couple of ways to potentially handle this, and CopyCat uses the snapshotting
method that is recommended by the authors of the Raft algorithm. However, in favor of simplicity,
the CopyCat snapshotting implementation does slightly differ from the one described in the Raft
paper. Rather than storing snapshots in a separate snapshot file, CopyCat stores snapshots as
normal log entries, making it easier to replicate snapshots when replicas fall too far out
of sync. Additionally, CopyCat guarantees that the first entry in any log will always be
a `SnapshotEntry`, again helping to ease the process of replicating snapshots to far
out-of-date replicas.

All snapshot serialization, storage, and loading is handled by CopyCat internally. Users
need only create and install the data via the `createSnapshot` and `installSnapshot` methods
respectively. Once the log grows to a predetermined size (configurable in `CopyCatConfig`),
CopyCat will take a snaphsot of the log and wipe all previous log entries.

```java
public class MyStateMachine implements StateMachine {
  private Map<String, Object> data = new HashMap<>();

  @Override
  public Snapshot takeSnapshot() {
    return new Snapshot(data);
  }

  @Override
  public void installSnapshot(Snapshot data) {
    this.data = data;
  }

  @Override
  public Object applyCommand(String command, Arguments args) {
    switch (command) {
      case "read":
        read(args);
      case "write":
        return write(args);
      default:
        throw new UnsupportedOperationException();
    }
  }

}
```

### Creating an annotated state machine
CopyCat provides a helpful base class which supports purely annotation based state
machines. To create an annotated state machine, simply extend the `AnnotatedStateMachine`
class.

```java
public class MyStateMachine extends AnnotatedStateMachine {
}
```

The annotated state machine will introspect itself to find commands defined with the
`@Command` annotation.

```java
public class MyStateMachine extends AnnotatedStateMachine {

  @Command(name="get", type=Command.Type.READ)
  public String get(@Command.Argument("key") String key) {
    return data.get(key);
  }

}
```

### Configuring the cluster
When a CopyCat cluster is first started, the cluster configuration must be explicitly
provided by the user. However, as the cluster runs, explicit cluster configurations
may no longer be required. This is because once a cluster leader has been elected,
the leader will replicate its cluster configuration to the rest of the cluster,
and the user-defined configuration will be replaced by an internal configuration.

To configure the CopyCat cluster, create a `ClusterConfig`.

```java
ClusterConfig cluster = new StaticClusterConfig();
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

### Creating a dynamic cluster
Dynamic cluster membership changes are supported via any `ClusterConfig` that
extends the `Observable` class. If a cluster configuration is `Observable`, the cluster
leader will observe the configuration once it is elected. When the configuration changes,
the leader will log and replicate the configuration change.

CopyCat provides an `Observable` cluster configuration called `DynamicClusterConfig`:

```java
ClusterConfig cluster = new DynamicClusterConfig();
```

### Creating the CopyCatContext
CopyCat replicas are run via a `CopyCatContext`. The CopyCat context is a container
for the replica's `Log`, `StateMachine`, and `ClusterConfig`. To start the replica,
simply call the `start()` method.

```java
CopyCatContext context = new CopyCatContext(stateMachine, cluster);
context.start();
```

### Setting the log type
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

### Submitting commands to the cluster
To submit commands to the CopyCat cluster, simply call the `submitCommand` method
on any `CopyCatContext`.

```java
Arguments args = new Arguments();
args.put("key", "foo");
context.submitCommand("read", args, new AsyncCallback<Object>() {
  @Override
  public void complete(Object result) {
    // Command succeeded.
  }
  @Override
  public void fail(Throwable t) {
    // Command failed.
  }
});
```

When a command is submitted to a `CopyCatContext`, the command will automatically
be forwarded on to the current cluster leader. Note that this behavior may change
to be altered by the *strategy* pattern at some point in the future.

## Protocols
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

`META-INF/services/net/kuujo/copycat/protocol/rest`

```
net.kuujo.copycat.protocol.impl.RestProtocol
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

### Writing a protocol client
The client's task is equally simple - to send messages to another replica when asked.
In order to do so, the `ProtocolClient` implements the other side of the `ProtocolServer`
callback methods.

```java
public interface ProtocolClient {

  void ping(PingRequest request, AsyncCallback<PingResponse> callback);

  void sync(SyncRequest request, AsyncCallback<SyncResponse> callback);

  void install(InstallRequest request, AsyncCallback<InstallResponse> callback);

  void poll(PollRequest request, AsyncCallback<PollResponse> callback);

  void submit(SubmitRequest request, AsyncCallback<SubmitResponse> callback);

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
* `@UriFragment`
* `@UriArgument`

Each of these annotations mirrors a method on the `URI` interface except for the
last one, `@UriArgument`. The `@UriArgument` annotation is a special annotation
for referencing parsed named query arguments.

URI annotations can be used either on protocol constructors or setter methods.
In either case, constructors or methods *must first be annotated with the
`@UriInject` annotation* in order to enable URI injection. Let's take a look
at an example of constructor injection:

```java
public class RestProtocol implements Protocol {
  private final String host;
  private final int port;
  private final String path;

  @UriInject
  public RestProtocol(@UriHost String host, @UriPort int port @UriPath String path) {
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
be constructing a `RestClient` instance within our `RestProtocol` constructor. We
can then create two constructors, one accepting a `host` and a `port` and one accepting
a `client`.

```java
public class RestProtocol implements Protocol {
  private final RestClient client;
  private final String path;

  @UriInject
  public RestProtocol(@UriArgument("client") RestClient client, @UriPath String path) {
    this.client = client;
    this.path = path;
  }

  @UriInject
  public RestProtocol(@UriHost String host, @UriPort int port @UriPath String path) {
    this(new RestClient(host, port), path);
  }

}
```

You may be interested in how CopyCat decides which constructor to use. Actually, it's
quite simple: the URI injector simply iterates over `@UriInject` annotated constructors
and attempts to construct the object from each one. If a given constructor cannot be
used due to a missing argument (such as the named `@UriArgument("client")`), the constructor
will be skipped. Once all constructors have been exhausted, the injector will again attempt
to fall back to a no-argument constructor.

Note also that the `@UriArgument("client")` annotation is referencing a `RestClient` object
which obviously can't exist within a raw URI string. Users can use a `Registry` instance
to register named objects that can then be referenced in URIs using the `#` prefix. For
instance:

```java
Registry registry = new BasicRegistry();
registry.bind("rest_client", new RestClient("localhost", 8080));
String uri = "rest://copycat?client=#rest_client";
```

The registry can then be passed to a `CopyCatContext` constructor.

```java
CopyCatContext context = new CopyCatContext(new MyStateMachine, cluster, registry);
```

When the URI query string is parsed, the parser will look for strings beginning with `#`
and use those strings to look up referenced objects in the context's registry.

### Using multiple URI annotations on a single parameter

URI schemas can often be inflexible for this type of use case, and users may want to
be able to back a parameter with multiple annotations. These two URIs will not parse
in the same way:

* `rest:copycat`
* `rest://copycat`

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
public RestProtocol(@UriAuthority @UriSchemeSpecificPart String path) {
  this.path = path;
}
```

### Making annotated URI parameters optional
In order to allow for more control over the way CopyCat selects constructors, users
can use the `@Optional` annotation to indicate that a `null` parameter can be ignored
if necessary. This will prevent CopyCat from skipping otherwise successful constructors.
For instance, in our `RestProtocol` example, the constructor could certainly take a
`host` without a `port`. Of course, we could simply create another constructor, but
maybe we just don't wan to :-)

```java
public class RestProtocol implements Protocol {

  @UriInject
  public RestProtocol(@UriHost String host, @Optional @UriPort int port) {
    this.host = host;
    this.port = port >= 0 ? port : 0;
  }

}
```

### The complete protocol
Now that we have all that out of the way, here's the complete `Protocol` implementation:

```java
public class RestProtocol implements Protocol {
  private RestClient client;
  private RestServer server;
  private String path;

  @UriInject
  public RestProtocol(@UriArgument("client") RestClient client, @UriArgument("server") RestServer server @UriAuthority String path) {
    this.client = client;
    this.server = server;
    this.path = path;
  }

  @UriInject
  public RestProtocol(@UriHost String host, @Optional @UriPort int port, @UriPath String path) {
    this.client = new RestClient(host, port);
    this.server = new RestServer(host, port);
    this.path = path;
  }

  @Override
  public void init(CopyCatContext context) {
  }

  @Override
  public ProtocolClient createClient() {
    return new RestProtocolClient(client, path);
  }

  @Override
  public ProtocolServer createServer() {
    return new RestProtocolServer(server, path);
  }

}
```

### Configuring the cluster with custom protocols

Let's take a look at an example of how to configure the CopyCat cluster when using
custom protocols.

```java
ClusterConfig cluster = new StaticClusterConfig("rest://localhost:8080/copycat");
cluster.addRemoteMember("rest://localhost:8081/copycat");
cluster.addRemoteMember("rest://localhost:8082/copycat");
```

Note that CopyCat does not particularly care about the protocol of any given node
in the cluster. Theoretically, different nodes could be connected together by any
protocol they want (though I can't imagine why one would want to do such a thing).
For the local replica, the protocol's server is used to receive messages. For remote
replicas, each protocol instance's client is used to send messages to those replicas.

## Built-in protocols
CopyCat maintains several built-in protocols, some of which are implemented on top
of asynchronous frameworks like Vert.x.

### Direct Protocol
The `direct` protocol is a simple protocol that communicates between contexts using
direct method calls. This protocol is intended purely for testing.

```java
Registry registry = new ConcurrentRegistry();
ClusterConfig cluster = new StaticClusterConfig("direct:foo");
cluster.setRemoteMembers("direct:bar", "direct:baz");
CopyCatContext context = new CopyCatContext(new MyStateMachine, cluster, registry);
```

Note that you should use a `ConcurrentRegistry` when using the `direct` protocol.

### Vert.x Event Bus Protocol
The Vert.x `eventbus` protocol communicates between replicas on the Vert.x event
bus. The event bus can either be created in a new `Vertx` instance or referenced
in an existing `Vertx` instance.

In order to use Vert.x protocols, you must add the `copycat-vertx` project as a dependency.

```java
ClusterConfig cluster = new StaticClusterConfig("eventbus://localhost:1234/foo");

// or...

ClusterConfig cluster = new StaticClusterConfig("eventbus://foo?vertx=#vertx");
Registry registry = new BasicRegistry();
registry.bind("vertx", vertx);
```

### Vert.x TCP Protocol
The Vert.x `tcp` protocol communicates between replicas using a simple wire protocol
over Vert.x `NetClient` and `NetServer` instances. To configure the `tcp` protocol,
simply use an ordinary TCP address.

In order to use Vert.x protocols, you must add the `copycat-vertx` project as a dependency.

```java
ClusterConfig cluster = new StaticClusterConfig("tcp://localhost:1234");
cluster.setRemoteMembers("tcp://localhost:1235", "tcp://localhost:1236");
```

## Endpoints
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
ClusterConfig cluster = new StaticClusterConfig("tcp://localhost:5555", "tcp://localhost:5556", "tcp://localhost:5557");
CopyCat copycat = new CopyCat("rest://localhost:8080", new MyStateMachine(), cluster)
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
CopyCat copycat = new CopyCat("eventbus://foo?vertx=#vertx", new MyStateMachine(), cluster, registry);
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
