CopyCat
=======

**[User Manual](#user-manual) | [Tutorials](#tutorials)**

CopyCat is an extensible Java-based implementation of the
[Raft consensus protocol](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

The core of CopyCat is a framework designed to support a variety of protocols and
transports. CopyCat provides a simple extensible API that can be used to build a
fault-tolerant state machine over TCP, HTTP, messaging systems, or any other
form of communication. The CopyCat Raft implementation supports advanced features
of the Raft algorithm such as snapshotting and dynamic cluster configuration changes.

User Manual
-----------

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

  Snapshot takeSnapshot();

  void installSnapshot(Snapshot snapshot);

  Object applyCommand(String command, Arguments args);

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
  @Stateful
  private Map<String, Object> data = new HashMap<>();

  @Command(name="get", type=Command.Type.READ)
  public Object get(@Command.Argument("key") String key) {
    return data.get(key);
  }

  @Command(name="set", type=Command.Type.WRITE)
  public void set(@Command.Argument("key") String key, @Command.Argument("value") Object value) {
    data.put(key, value);
  }

  @Command(name="delete", type=Command.Type.READ_WRITE)
  public Object delete(@Command.Argument("key") String key) {
    return data.remove(key);
  }

}
```

Note also that the `@Stateful` annotation is used to indicate that the `data` field
should be persisted whenever a snapshot is taken.

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

  void installSnapshot(InstallSnapshotRequest request, AsyncCallback<InstallSnapshotResponse> callback);

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
  public HttpProtocol(@UriArgument("client") HttpClient client, @UriPath String path) {
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
used due to a missing argument (such as the named `@UriArgument("client")`), the constructor
will be skipped. Once all constructors have been exhausted, the injector will again attempt
to fall back to a no-argument constructor.

Note also that the `@UriArgument("client")` annotation is referencing a `HttpClient` object
which obviously can't exist within a raw URI string. Users can use a `Registry` instance
to register named objects that can then be referenced in URIs using the `#` prefix. For
instance:

```java
Registry registry = new BasicRegistry();
registry.bind("http_client", new HttpClient("localhost", 8080));
String uri = "http://copycat?client=#http_client";
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

### The complete protocol
Now that we have all that out of the way, here's the complete `Protocol` implementation:

```java
public class HttpProtocol implements Protocol {
  private HttpClient client;
  private HttpServer server;
  private String path;

  @UriInject
  public HttpProtocol(@UriArgument("client") HttpClient client, @UriArgument("server") HttpServer server @UriAuthority String path) {
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
ClusterConfig cluster = new StaticClusterConfig("http://localhost:8080/copycat");
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

Tutorials
---------

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
named `Arguments`. The `AnnotatedStateMachine` will automatically validate argument types
against user provided arguments according to parameter annotations.

```java
public class KeyValueStore extends AnnotatedStateMachine {

  @Stateful
  private final Map<String, Object> data = new HashMap<>();

  @Command(name="get", type=Command.Type.READ)
  public Object get(@Argument("key") String key) {
    return data.get(key);
  }

  @Command(name="set", type=Command.Type.WRITE)
  public void set(@Argument("key") String key, @Argument("value") Object value) {
    data.put(key, value);
  }

  @Command(name="delete", type=Command.Type.WRITE)
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
ClusterConfig cluster = new StaticClusterConfig();
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
          socket.dataHandler(RecordParser.newDelimited(new byte[]{'\00'}, new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
              JsonObject request = new JsonObject(buffer.toString());
              String type = request.getString("type");
              if (type != null) {
                switch (type) {
                  case "append":
                    handleAppendRequest(socket, request);
                    break;
                  case "install":
                    handleInstallRequest(socket, request);
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
            callback.fail(result.cause());
          } else {
            callback.complete(null);
          }
        }
      });
    } else {
      callback.complete(null);
    }
  }

  /**
   * Handles an append entries request.
   */
  private void handleAppendRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final long id = request.getLong("id");
      List<Entry> entries = new ArrayList<>();
      JsonArray jsonEntries = request.getArray("entries");
      if (jsonEntries != null) {
        for (Object jsonEntry : jsonEntries) {
          entries.add(serializer.readValue(jsonEntry.toString().getBytes(), Entry.class));
        }
      }
      requestHandler.appendEntries(new AppendEntriesRequest(request.getLong("term"), request.getString("leader"), request.getLong("prevIndex"), request.getLong("prevTerm"), entries, request.getLong("commit")), new AsyncCallback<AppendEntriesResponse>() {
        @Override
        public void complete(AppendEntriesResponse response) {
          if (response.status().equals(Response.Status.OK)) {
            respond(socket, new JsonObject().putString("status", "ok").putNumber("id", id).putNumber("term", response.term()).putBoolean("succeeded", response.succeeded()));
          } else {
            respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", response.error().getMessage()));
          }
        }
        @Override
        public void fail(Throwable t) {
          respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", t.getMessage()));
        }
      });
    }
  }

  /**
   * Handles an install request.
   */
  private void handleInstallRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final long id = request.getLong("id");
      Set<String> cluster = new HashSet<>();
      JsonArray jsonNodes = request.getArray("cluster");
      if (jsonNodes != null) {
        for (Object jsonNode : jsonNodes) {
          cluster.add(jsonNode.toString());
        }
      }
      requestHandler.installSnapshot(new InstallSnapshotRequest(request.getLong("term"), request.getString("leader"), request.getLong("snapshotIndex"), request.getLong("snapshotTerm"), cluster, request.getBinary("data"), request.getBoolean("complete")), new AsyncCallback<InstallSnapshotResponse>() {
        @Override
        public void complete(InstallSnapshotResponse response) {
          if (response.status().equals(Response.Status.OK)) {
            respond(socket, new JsonObject().putString("status", "ok").putNumber("id", id).putNumber("term", response.term()).putBoolean("succeeded", response.succeeded()));
          } else {
            respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", response.error().getMessage()));
          }
        }
        @Override
        public void fail(Throwable t) {
          respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", t.getMessage()));
        }
      });
    }
  }

  /**
   * Handles a vote request.
   */
  private void handleVoteRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final long id = request.getLong("id");
      requestHandler.requestVote(new RequestVoteRequest(request.getLong("term"), request.getString("candidate"), request.getLong("lastIndex"), request.getLong("lastTerm")), new AsyncCallback<RequestVoteResponse>() {
        @Override
        public void complete(RequestVoteResponse response) {
          if (response.status().equals(Response.Status.OK)) {
            respond(socket, new JsonObject().putString("status", "ok").putNumber("id", id).putNumber("term", response.term()).putBoolean("voteGranted", response.voteGranted()));
          } else {
            respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", response.error().getMessage()));
          }
        }
        @Override
        public void fail(Throwable t) {
          respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", t.getMessage()));
        }
      });
    }
  }

  /**
   * Handles a submit request.
   */
  private void handleSubmitRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final long id = request.getLong("id");
      requestHandler.submitCommand(new SubmitCommandRequest(request.getString("command"), new Arguments(request.getObject("args").toMap())), new AsyncCallback<SubmitCommandResponse>() {
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void complete(SubmitCommandResponse response) {
          if (response.status().equals(Response.Status.OK)) {
            if (response.result() instanceof Map) {
              respond(socket, new JsonObject().putString("status", "ok").putNumber("id", id).putObject("result", new JsonObject((Map) response.result())));
            } else if (response.result() instanceof List) {
              respond(socket, new JsonObject().putString("status", "ok").putNumber("id", id).putArray("result", new JsonArray((List) response.result())));
            } else {
              respond(socket, new JsonObject().putString("status", "ok").putNumber("id", id).putValue("result", response.result()));
            }
          } else {
            respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", response.error().getMessage()));
          }
        }
        @Override
        public void fail(Throwable t) {
          respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", t.getMessage()));
        }
      });
    }
  }

  /**
   * Responds to a request from the given socket.
   */
  private void respond(NetSocket socket, JsonObject response) {
    socket.write(response.encode() + '\00');
  }

  @Override
  public void stop(final AsyncCallback<Void> callback) {
    if (server != null) {
      server.close(new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            callback.fail(result.cause());
          } else {
            callback.complete(null);
          }
        }
      });
    } else {
      callback.complete(null);
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
  private long id;
  private final Map<Long, ResponseHolder<?>> responses = new HashMap<>();

  /**
   * Holder for response handlers.
   */
  private static class ResponseHolder<T extends Response> {
    private final AsyncCallback<T> callback;
    private final ResponseType type;
    private final long timer;
    private ResponseHolder(long timerId, ResponseType type, AsyncCallback<T> callback) {
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
    INSTALL,
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
      long requestId = ++id;
      JsonArray jsonEntries = new JsonArray();
      for (Entry entry : request.entries()) {
        jsonEntries.addString(new String(serializer.writeValue(entry)));
      }
      socket.write(new JsonObject().putString("type", "append")
          .putNumber("id", requestId)
          .putNumber("term", request.term())
          .putString("leader", request.leader())
          .putNumber("prevIndex", request.prevLogIndex())
          .putNumber("prevTerm", request.prevLogTerm())
          .putArray("entries", jsonEntries)
          .putNumber("commit", request.commitIndex()).encode() + '\00');
      storeCallback(requestId, ResponseType.APPEND, callback);
    } else {
      callback.fail(new ProtocolException("Client not connected"));
    }
  }

  @Override
  public void installSnapshot(InstallSnapshotRequest request, AsyncCallback<InstallSnapshotResponse> callback) {
    if (socket != null) {
      long requestId = ++id;
      JsonArray jsonCluster = new JsonArray();
      for (String member : request.cluster()) {
        jsonCluster.addString(member);
      }
      socket.write(new JsonObject().putString("type", "install")
          .putNumber("id", requestId)
          .putNumber("term", request.term())
          .putString("leader", request.leader())
          .putArray("cluster", jsonCluster)
          .putBinary("data", request.data())
          .putBoolean("complete", request.complete())
          .encode() + '\00');
      storeCallback(requestId, ResponseType.INSTALL, callback);
    } else {
      callback.fail(new ProtocolException("Client not connected"));
    }
  }

  @Override
  public void requestVote(RequestVoteRequest request, AsyncCallback<RequestVoteResponse> callback) {
    if (socket != null) {
      long requestId = ++id;
      socket.write(new JsonObject().putString("type", "vote")
          .putNumber("id", requestId)
          .putNumber("term", request.term())
          .putString("candidate", request.candidate())
          .putNumber("lastIndex", request.lastLogIndex())
          .putNumber("lastTerm", request.lastLogTerm())
          .encode() + '\00');
      storeCallback(requestId, ResponseType.VOTE, callback);
    } else {
      callback.fail(new ProtocolException("Client not connected"));
    }
  }

  @Override
  public void submitCommand(SubmitCommandRequest request, AsyncCallback<SubmitCommandResponse> callback) {
    if (socket != null) {
      long requestId = ++id;
      socket.write(new JsonObject().putString("type", "submit")
          .putString("command", request.command())
          .putObject("args", new JsonObject(request.args()))
          .encode() + '\00');
      storeCallback(requestId, ResponseType.SUBMIT, callback);
    } else {
      callback.fail(new ProtocolException("Client not connected"));
    }
  }

  /**
   * Handles an identifiable response.
   */
  @SuppressWarnings("unchecked")
  private void handleResponse(long id, JsonObject response) {
    ResponseHolder<?> holder = responses.remove(id);
    if (holder != null) {
      vertx.cancelTimer(holder.timer);
      switch (holder.type) {
        case APPEND:
          handleAppendResponse(response, (AsyncCallback<AppendEntriesResponse>) holder.callback);
          break;
        case INSTALL:
          handleInstallResponse(response, (AsyncCallback<InstallSnapshotResponse>) holder.callback);
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
      callback.fail(new ProtocolException("Invalid response"));
    } else if (status.equals("ok")) {
      callback.complete(new AppendEntriesResponse(response.getLong("term"), response.getBoolean("succeeded")));
    } else if (status.equals("error")) {
      callback.fail(new ProtocolException(response.getString("message")));
    }
  }

  /**
   * Handles an install response.
   */
  private void handleInstallResponse(JsonObject response, AsyncCallback<InstallSnapshotResponse> callback) {
    String status = response.getString("status");
    if (status == null) {
      callback.fail(new ProtocolException("Invalid response"));
    } else if (status.equals("ok")) {
      callback.complete(new InstallSnapshotResponse(response.getLong("term"), response.getBoolean("succeeded")));
    } else if (status.equals("error")) {
      callback.fail(new ProtocolException(response.getString("message")));
    }
  }

  /**
   * Handles a vote response.
   */
  private void handleVoteResponse(JsonObject response, AsyncCallback<RequestVoteResponse> callback) {
    String status = response.getString("status");
    if (status == null) {
      callback.fail(new ProtocolException("Invalid response"));
    } else if (status.equals("ok")) {
      callback.complete(new RequestVoteResponse(response.getLong("term"), response.getBoolean("voteGranted")));
    } else if (status.equals("error")) {
      callback.fail(new ProtocolException(response.getString("message")));
    }
  }

  /**
   * Handles a submit response.
   */
  private void handleSubmitResponse(JsonObject response, AsyncCallback<SubmitCommandResponse> callback) {
    String status = response.getString("status");
    if (status == null) {
      callback.fail(new ProtocolException("Invalid response"));
    } else if (status.equals("ok")) {
      callback.complete(new SubmitCommandResponse(response.getObject("result").toMap()));
    } else if (status.equals("error")) {
      callback.fail(new ProtocolException(response.getString("message")));
    }
  }

  /**
   * Stores a response callback by ID.
   */
  private <T extends Response> void storeCallback(final long id, ResponseType responseType, AsyncCallback<T> callback) {
    long timerId = vertx.setTimer(30000, new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        ResponseHolder<?> holder = responses.remove(id);
        if (holder != null) {
          holder.callback.fail(new ProtocolException("Request timed out"));
        }
      }
    });
    ResponseHolder<T> holder = new ResponseHolder<T>(timerId, responseType, callback);
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
              callback.fail(result.cause());
            }
          } else {
            socket = result.result();
            socket.dataHandler(RecordParser.newDelimited(new byte[]{'\00'}, new Handler<Buffer>() {
              @Override
              public void handle(Buffer buffer) {
                JsonObject response = new JsonObject(buffer.toString());
                long id = response.getLong("id");
                handleResponse(id, response);
              }
            }));
            if (callback != null) {
              callback.complete(null);
            }
          }
        }
      });
    } else if (callback != null) {
      callback.complete(null);
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
            callback.complete(null);
          }
        }
      }).close();
    } else if (client != null) {
      client.close();
      client = null;
      if (callback != null) {
        callback.complete(null);
      }
    } else if (callback != null) {
      callback.complete(null);
    }
  }

}
```

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
into a `command` and `Arguments` and calling `submitCommand` on the local
`CopyCatContext`. CopyCat will internally handle routing of commands to the
appropriate node for logging and replication.

```java
public class HttpEndpoint implements Endpoint {
  private final Vertx vertx;
  private CopyCatContext context;
  private HttpServer server;
  private final String host;
  private final int port;

  @UriInject
  public HttpEndpoint(@UriHost String host, @Optional @UriPort int port) {
    this.host = host;
    this.port = port;
    this.vertx = new DefaultVertx();
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
            // Submit the command to the CopyCat cluster.
            HttpEndpoint.this.context.submitCommand(request.params().get("command"), new Arguments(new JsonObject(buffer.toString()).toMap()), new AsyncCallback<Object>() {
              @Override
              @SuppressWarnings({"unchecked", "rawtypes"})
              public void complete(Object result) {
                request.response().setStatusCode(200);
                if (result instanceof Map) {
                  request.response().end(new JsonObject().putString("status", "ok").putString("leader", HttpEndpoint.this.context.leader()).putObject("result", new JsonObject((Map) result)).encode());                  
                } else if (result instanceof List) {
                  request.response().end(new JsonObject().putString("status", "ok").putString("leader", HttpEndpoint.this.context.leader()).putArray("result", new JsonArray((List) result)).encode());
                } else {
                  request.response().end(new JsonObject().putString("status", "ok").putString("leader", HttpEndpoint.this.context.leader()).putValue("result", result).encode());
                }
              }
              @Override
              public void fail(Throwable t) {
                request.response().setStatusCode(400);
              }
            });
          }
        });
      }
    });
    server.requestHandler(routeMatcher);
  }

  @Override
  public void start(final AsyncCallback<Void> callback) {
    server.listen(port, host, new Handler<AsyncResult<HttpServer>>() {
      @Override
      public void handle(AsyncResult<HttpServer> result) {
        if (result.failed()) {
          callback.fail(result.cause());
        } else {
          callback.complete(null);
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
          callback.fail(result.cause());
        } else {
          callback.complete(null);
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
ClusterConfig cluster = new StaticClusterConfig();
cluster.setLocalMember("tcp://localhost:5005");
cluster.addRemoteMember("tcp://localhost:5006");
cluster.addRemoteMember("tcp://localhost:5007");

CopyCat copycat = new CopyCat("http://locahost:8080", new KeyValueStore(), new FileLog("key-value.log"), cluster);
copycat.start();
```
