Copycat
=======

[![Build Status](https://travis-ci.org/kuujo/copycat.png)](https://travis-ci.org/kuujo/copycat)

Copycat is an extensible log-based distributed coordination framework for Java 8 built on the
[Raft consensus protocol](https://raftconsensus.github.io/).

#### [User Manual](#user-manual)

Copycat is a strongly consistent embedded distributed coordination framework built on the
[Raft consensus protocol](https://raftconsensus.github.io/). Copycat exposes a set of high level APIs with tools to
solve a variety of distributed systems problems including:
* [Replicated state machines](#state-machines)
* [Distributed collections](#collections)
* [Distributed atomic variables](#atomic-variables)

The newest version of Copycat is 0.6 and is still under development. Over the coming weeks, new resources will
be added to Copycat's high-level API including:
* Distributed locks
* Leader elections
* State change listeners

**Copycat is still undergoing heavy development and testing and is therefore not recommended for production!**

[Jepsen](https://github.com/aphyr/jepsen) tests are [currently being developed](http://github.com/jhalterman/copycat-jepsen)
to verify the stability of Copycat in an unreliable distributed environment. Thus far, Copycat has passed a number
of Jepsen tests to verify that linearizability is maintained for both writes and reads during network partitions.
However, there is still work to be done, and Copycat will not be fully released until significant testing is done
both via normal testing frameworks and Jepsen. In the meantime, Copycat snapshots will be pushed, and a beta release
of Copycat is expected within the coming weeks. Follow the project for updates!

*Copycat requires Java 8*

User Manual
===========

Documentation is currently undergoing a complete rewrite due to recent changes. Docs should be updated frequently until
a release, so check back for more! If you would like to request specific documentation, please
[submit a request](http://github.com/kuujo/copycat/issues).

1. [Getting started](#getting-started)
   * [Working with servers](#working-with-servers)
   * [Working with clients](#working-with-clients)
   * [Working with resources](#working-with-resources)
   * [Distributed collections](#distributed-collections)
      * [Read consistency](#read-consistency)
      * [Expiring keys and values](#expiring-keys-and-values)
      * [Ephemeral keys and values](#ephemeral-keys-and-values)
1. [Custom resources](#custom-resources)
   * [State machines](#state-machines)
   * [Operations](#operations)
      * [Commands](#commands)
      * [Queries](#queries)
   * [Filtering](#filtering)
1. [Serialization](#serialization)
   * [Registering serializers](#registering-serializers)
   * [Buffers](#buffers)
   * [ObjectWriter](#objectwriter)
   * [Writable](#writable)
   * [Reference counting](#reference-counting)

## Getting started

Copycat is a framework for consistent distributed coordination. At the core of Copycat is a reusable implementation
of the Raft consensus protocol. On top of Raft, Copycat provides a high level path-based API for creating and
interacting with arbitrary replicated state machines such as maps, sets, locks, or user-defined resources. Resources
can be created and operated on by any node in the cluster.

Copycat clusters consist of at least one [server](#working-with-servers) and any number of [clients](#working-with-clients).
For fault-tolerance, it is recommended that each Copycat cluster have 3 or 5 servers, and the number of servers should
always be odd in order to achieve the greatest level of fault-tolerance.

All network driven interfaces in Copycat are asynchronous and make heavy use of Java 8's `CompletableFuture`.
At the core of Copycat is the `Copycat` API, which is exposed by both servers and clients.

*Synchronous use*
```java
Copycat copycat = CopycatClient.builder()
  .withMembers(...)
  .build()
  .open()
  .get();

Node node = copycat.create("/test").get();

AsyncMap<String, String> map = node.create(AsyncMap.class).get();

map.put("foo", "Hello world!").get();

assert map.get("foo").get().equals("Hello world!");
```

*Asynchronous use*
```java
Copycat copycat = CopycatClient.builder()
  .withMembers(...)
  .build();

copycat.create("/test").thenAccept(node -> {
  node.create(AsyncMap.class).thenAccept(map -> {
    map.put("foo", "Hello world!").thenRun(() -> {
      map.get("foo").thenAccept(result -> {
        assert result.equals("Hello world!");
      });
    });
  });
});
```

More on [working with resources](#working-with-resources) below.

### Working with servers

The `CopycatServer` class provides a `Copycat` implementation that participates in Raft-based replication of state.
As with [clients](#working-with-clients), servers operate as normal members of the Copycat cluster, allowing users
to create and use resources.

To construct a `CopycatServer` instance, use the `CopycatServer.Builder`:

```java
CopycatServer.Builder builder = CopycatServer.builder();
```

#### Configuring the cluster

Each `CopycatServer` instance requires a `Cluster`. To set up the `Cluster`, use a `Cluster.Builder`.

```java
Cluster.Builder clusterBuilder = NettyCluster.builder();
```

Each server in the Copycat cluster must be identified by a unique numeric identifier. To configure the server ID,
call `withMemberId` on the cluster builder:

```java
clusterBuilder.withMemberId(1);
```

The server ID *must remain constant through failures*. Some systems like ZooKeeper maintain consistency for the server
ID by storing it in a file. Copycat is agnostic about how configuration is persisted across server restarts.

Once the cluster member ID has been configured, you must add a set of members to the cluster. The `Copycat` instance
will use this membership list to connect to and communicate with other nodes in the cluster.

Copycat supports two types of replicating members (or servers) - `ACTIVE` and `PASSIVE`.

Active members are Raft voting members and must be listed in the membership list on each server. Copycat does not
currently support runtime changes to the active membership list, so each server's membership list should be identical.

```java
clusterBuilder.withMemberId(1)
  .addMember(NettyMember.builder()
    .withId(1)
    .withHost(InetAddress.getByName("123.456.789.1").getHostName())
    .withPort(5050)
    .build())
  .addMember(NettyMember.builder()
    .withId(1)
    .withHost(InetAddress.getByName("123.456.789.2").getHostName())
    .withPort(5050)
    .build())
  .addMember(NettyMember.builder()
    .withId(1)
    .withHost(InetAddress.getByName("123.456.789.3").getHostName())
    .withPort(5050)
    .build())
  .build();
```

Whether the server being constructed is a Raft voting member is determined by whether its `id` is listed in the configured
membership list. In the example above, the local ID is `1`, and that member is also listed in the membership list,
so the server will be started as a Raft voting member.

On the other hand, passive members are participate in replication asynchronously and *should not be listed in the
Raft membership list.*

```java
clusterBuilder.withMemberId(5)
  .addMember(NettyMember.builder()
    .withId(1)
    .withHost(InetAddress.getByName("123.456.789.1").getHostName())
    .withPort(5050)
    .build())
  .addMember(NettyMember.builder()
    .withId(1)
    .withHost(InetAddress.getByName("123.456.789.2").getHostName())
    .withPort(5050)
    .build())
  .addMember(NettyMember.builder()
    .withId(1)
    .withHost(InetAddress.getByName("123.456.789.3").getHostName())
    .withPort(5050)
    .build())
  .build();
```

*Note that Copycat requires a majority of servers to acknowledge a write, so your cluster should always have at
least 3 Raft voting members, and the number of voting members should always be odd.*

Up until now we've seen how to configure the `CopycatServer` with a cluster configuration. Let's see how that looks:

```java
Copycat copycat = CopycatServer.builder()
  .withCluster(NettyCluster.builder()
    .withMemberId(1)
    .addMember(NettyMember.builder()
      .withMemberId(1)
      .withHost(InetAddress.getByName("123.456.789.1").getHostName())
      .withPort(5050)
      .build())
    .addMember(NettyMember.builder()
      .withMemberId(2)
      .withHost(InetAddress.getByName("123.456.789.2").getHostName())
      .withPort(5050)
      .build())
    .addMember(NettyMember.builder()
      .withMemberId(3)
      .withHost(InetAddress.getByName("123.456.789.3").getHostName())
      .withPort(5050)
      .build())
    .build())
  .build();
```

This example will throw a `ConfigurationException` because the server has not yet been configured with a `Log`.

#### Configuring the log

Each `CopycatServer` communicates with other servers in the cluster to replicate state changes through a persistent
log. To configure the log for the server, create a `Log.Builder`:

```java
Log.Builder logBuilder = Log.builder();
```

The log supports two forms of storage via a configurable `StorageLevel`. The most common form of log persistence is
`DISK`. When the log is configured for `DISK` storage, you should also provide a log directory:

```java
logBuilder.withStorageLevel(StorageLevel.DISK)
  .withDirectory("data/logs");
```

Alternatively, Copycat can store data strictly in off-heap memory by configuring the log with the `MEMORY` storage level:

```java
logBuilder.withStorageLevel(StorageLevel.MEMORY);
```

#### Putting it all together

Put together, the `CopycatServer` can be constructed in a single builder statement:

```java
Copycat copycat = CopycatServer.builder()
  .withCluster(NettyCluster.builder()
    .withMemberId(1)
    .addMember(NettyMember.builder()
      .withMemberId(1)
      .withHost("123.456.789.1")
      .withPort(5050)
      .build())
    .addMember(NettyMember.builder()
      .withMemberId(2)
      .withHost("123.456.789.2")
      .withPort(5050)
      .build())
    .addMember(NettyMember.builder()
      .withMemberId(3)
      .withHost("123.456.789.3")
      .withPort(5050)
      .build())
    .build())
  .withLog(Log.builder()
    .withDirectory("log/data")
    ,build())
  .build();
```

Once the server has been created, open the server by calling `Copycat.open`. As with all user facing interfaces, the `Copycat` API is asynchronous and returns `CompletableFuture` for asynchronous
operations. `Copycat.open` will return a `CompletableFuture` that will be completed in a background thread once the
server has been opened.

```java
copycat.open().thenRun(() -> {
  System.out.println("Server open!");
});
```

Note that Copycat will *always* complete futures in the same background thread.

If you want to block until the server is opened, call `get()` or `join()` on the returned future.

```java
copycat.open().get();
```

Once the server is open, you can use the `Copycat` instance as a client to create paths and resources and perform
other operations:

```java
AsyncMap<String, String> map = copycat.create("/test", AsyncMap.class).get();
map.put("foo", "Hello world!").get();
```

### Working with clients

Clients work similarly to servers in that they take a set of Raft members with which to communicate via a builder.
In contrast to servers, though, clients do not require a unique member ID or a `Log` as they do not store state. This
means that all operations submitted by a client node will be executed remotely.

When constructing a client, the user must provide a set of `Members` to which to connect the client. `Members` is a parent
type of `Cluster` which lists only remote members.

```java
Copycat copycat = CopycatClient.builder()
  .withMembers(NettyMembers.builder()
    .addMember(NettyMember.builder()
      .withMemberId(1)
      .withHost("123.456.789.1")
      .withPort(5050)
      .build())
    .addMember(NettyMember.builder()
      .withMemberId(2)
      .withHost("123.456.789.2")
      .withPort(5050)
      .build())
    .addMember(NettyMember.builder()
      .withMemberId(3)
      .withHost("123.456.789.3")
      .withPort(5050)
      .build())
    .build())
  .build();
```

Once the client has been created, open the client by calling `Copycat.open`. As with all user facing interfaces, the `Copycat` API is asynchronous and returns `CompletableFuture` for asynchronous
operations. `Copycat.open` will return a `CompletableFuture` that will be completed in a background thread once the
server has been opened.

```java
copycat.open().thenRun(() -> {
  System.out.println("Server open!");
});
```

Note that Copycat will *always* complete futures in the same background thread.

If you want to block until the server is opened, call `get()` or `join()` on the returned future.

```java
copycat.open().get();
```

Once the server is open, you can use the `Copycat` instance to create paths and resources and perform other operations:

```java
AsyncMap<String, String> map = copycat.create("/test", AsyncMap.class).get();
map.put("foo", "Hello world!").get();
```

### Working with resources

The high level `Copycat` API provides a path oriented interface that allows users to define arbitrary named resources.
Resources are server-side state machines that are replicated via the Raft consensus protocol. A resource instance provides
a class for submitting operations to the Copycat cluster and an associated state machine for managing the server-side
state of a resource. For instance, the `AsyncMap` resource submits operations to a replicated `AsyncMap.StateMachine`
state machine implementation. The `AsyncMap.StateMachine` holds the map state in memory on each Copycat server.

Copycat allows resources to be associated with hierarchical paths via the `Copycat` interface. To define a path, create
a `Node` instance via `Copycat.create`

```java
Node node = copycat.create("/test").get();
```

Nodes may or may not have an associated resource, but no node can have more than one resource. This decision was made
because Copycat support arbitrary state machines as resources, so complex patterns can be modeled in custom state machines
rather than in restrictive abstractions.

To create a resource at a specific path, call `create()` again either on a `Node` or the `Copycat` instance itself,
passing the `Resource` class object:

```java
AsyncReference<Long> ref = node.create(AsyncReference.class).get();
```

Internally, a state machine will be created for the resource on each Copycat server.

For instance, when an `AsyncMap` resource (provided in the `copycat-collections` submodule) is created, the resource
is submitted to the Copycat cluster where an `AsyncMap.StateMachine` instance is created on each Raft server. As
operations are performed on the `AsyncMap`, those operations are submitted to and replicated through the cluster
and ultimately applied to replicas of the map's state machine on each Raft server.

### Distributed collections

Copycat provides basic distributed collections via the `copycat-collections` module. To use collections, users must
ensure that the collections dependency is added *to all servers* and any clients that access collection resources.

```
<dependency>
  <groupId>net.kuujo.copycat</groupId>
  <artifactId>copycat-collections</artifactId>
  <version>0.6.0-SNAPSHOT</version>
</dependency>
```

To create a collection, simply pass the collection class to the `create` method:

```java
AsyncMap<String, String> map = copycat.create("/test-map", AsyncMap.class);
```

You can then use the map to write and read map keys:

```java
map.put("foo", "Hello world!").thenRun(() -> {
  map.get("foo").thenAccept(value -> {
    assert value.equals("Hello world!");
  });
});
```

#### Read consistency

When performing operations on resources, Copycat separates the types of operations into two categories - commands
and queries. Due to the nature of the Raft algorithm, commands (writes) must always go through the Raft cluster
and be replicated prior to completion. For instance, when `AsyncMap.put` is called, the `Put` operation will be submitted
to the Copycat cluster and ultimately replicated prior to the response.

However, the Raft consensus algorithm allows for some optimizations for queries (reads) at the expense of consistency.
When read operations - e.g. `AsyncMap.get` or `AsyncMap.size` - are performed on resources, Copycat allows the user
to specify the required level of consistency and optimizes the read according to the user defined consistency level.

The four consistency levels available are:
* `LINEARIZABLE_STRICT` - Provides guaranteed linearizability by forcing all reads to go through the leader and
  verifying leadership with a majority of the Raft cluster during queries
* `LINEARIZABLE_LEASE` - Provides best-effort optimized linearizability by forcing all reads to go through the leader
  but allowing most queries to be executed without contacting a majority of the cluster so long as less than the
  election timeout has passed since the last time the leader communicated with a majority
* `SEQUENTIAL` - Provides sequential consistency by allowing clients to read from followers and `PASSIVE` members but
  ensuring that state does not go back in time from the perspective of any given client
* `SERIALIZABLE` - Provides serializable consistency by allowing clients to read from followers and `PASSIVE` members
  without any additional checks

All query operations for collections and other resources provide an overloaded method for specifying query consistency
level:

```java
map.put("foo", "Hello world!").thenRun(() -> {
  map.get("foo", ConsistencyLevel.SEQUENTIAL).thenAccept(value -> {
    assert value.equals("Hello world!");
  });
});
```

#### Expiring keys and values

Collections and other resources support time-based state changes such as expiring keys via TTLs.

Both `AsyncMap` and `AsyncSet` support expiring keys and values respectively. To set an expiring key, simply pass
a `ttl` to any state changing operation:

```java
map.put("foo", "Hello world!", 1, TimeUnit.SECONDS).thenRun(() -> {
  Thread.sleep(2);
  map.get("foo").thenAccept(value) -> {
    assert value == null;
  });
});
```

Note that timers start at some point between the command's invocation and completion. In other words, if an operation
to set a key with a TTL of 1 second takes 100 milliseconds to complete, the actual expiration of the key could take
place between 1000 and 1100 milliseconds of the completion of the operation.

#### Ephemeral keys and values

In addition to supporting time-based state changes, collections and other resources also support session based changes.
Each Copycat client and server maintains a session with the cluster leader throughout its lifetime. In the event that
the client or server is closed, dies, or is partitioned from the rest of the cluster, the session will expire, and
state machines can react to the expiration of sessions.

Both `AsyncMap` and `AsyncSet` make use of sessions to provide ephemeral keys and values respectively. To set an
ephemeral key, simply pass `Mode.EPHEMERAL` to any state change operation:

```java
copycat.open().get();

AsyncMap<String, String> map = copycat.create("/test", AsyncMap.class).get();

map.put("foo", "Hello world!", Mode.EPHEMERAL).get();

copycat.close().get();
```

In the example above, once the Copycat instance is closed and the client has disconnected from the other servers, the
client's session will expire and the `foo` key will be evicted from the map.

## Custom resources

The Copycat API is designed to facilitate operating on arbitrary user-defined resources. When a custom resource is created
via `Copycat.create`, an associated state machine will be created on each Copycat server, and operations submitted by the
resource instance will be applied to the replicated state machine. In that sense, we can think of a `Resource` instance
as a client-side object and a `StateMachine` instance as the server-side representation of that object.

To define a new resource, simply extend `AbstractResource`:

```java
public class Value extends AbstractResource {

}
```

### State machines

Each resource *must be associated with a state machine* implementation. When a resource is created, Copycat will instantiate
an instance of the resource's `StateMachine` on each node in the Copycat cluster. When methods are called on the client-side
resource instance, operations will be submitted to the Copycat cluster, replicated via the Raft consensus algorithm, and
applied to the resource's state machine on each node in the cluster.

To define a state machine, extend the base `StateMachine` class:

```java
public class ValueStateMachine extends StateMachine {

}
```

Once a state machine has been created, it must be associated with a resource type by annotating the resource with the
`@Stateful` annotation:

```java
@Stateful(ValueStateMachine.class)
public class Value<T> extends AbstractResource {

}
```

Any attempt to create a `Resource` without the `@Stateful` annotation will result in an exception.

### Operations

Resources are represented in terms of ` client side `Resource` instance and a set of server-side `StateMachine` instances.
In order to alter the state of a resource - e.g. to set the value of our `Value` resource - an operation must be submitted
to the cluster. Operations come in two forms [commands](#commands) and [queries](#queries).

All operations extend the base `Operation` interface which implements `Serializable`. This means that Copycat can natively
serialize all operations. However, for the best performance users should implement [Writable](#writable) or create an
[ObjectWriter](#objectwriter) for operations.

#### Commands

Commands are operations that modify the state of a resource. When a command operation is submitted to the Copycat cluster,
the command is logged to disk or memory (depending on the `Log` configuration) and replicated via the Raft consensus
protocol. Once the command has been stored on a majority of `ACTIVE` cluster members, it will be applied to the resource's
server-side `StateMachine` and the output will be returned to the client.

Commands are defined by implementing the `Command` interface:

```java
public class Set<T> implements Command<T> {
  private final String value;

  public Set(String value) {
    this.value = value;
  }
}
```

When implementing a command for a resource, the resource's `StateMachine` must have a mechanism for handling the command.
Commands are handled by simply annotating a `public` or `protected` state machine method with the `@Apply` annotation.
The base `StateMachine` class will apply operations to internal methods given the `@Apply` annotation:

```java
public class ValueStateMachine extends StateMachine {
  private Object value;

  /**
   * Set a new value and return the old value.
   */
  @Apply(Set.class)
  public Object set(Object value) {
    Object oldValue = this.value;
    this.value = value;
    return oldValue;
  }

}
```

Once the server-side state machine has been modified to handle the application of a command, we must alter the `Resource`
implementation to handle submitting the command to the Copycat cluster. This is done by simply calling the protected
`submit` method.

```java
@Stateful(ValueStateMachine.class)
public class Value<T> extends AbstractResource {

  /**
   * Sets the value of the resource.
   */
  public CompletableFuture<T> set(T value) {
    return submit(new Set<>(value));
  }
}
```

When the `submit` method is called, the parent `AbstractResource` will submit the command to the Raft cluster where it
will be persisted and replicated to a majority of the cluster before being applied to the leader's state machine. The
state machine's return value will be returned to the client.

The `submit` method always returns a `CompletableFuture`, however there is no requirement that resources be asynchronous.
Users can implement synchronous resources by simply blocking on `submit` calls:

```java
@Stateful(ValueStateMachine.class)
public class Value<T> extends AbstractResource {

   /**
    * Sets the value of the resource.
    */
   public T set(T value) {
     try {
       return submit(new Set<>(value)).get();
     } catch (InterruptedException e) {
       throw new RuntimeException(e);
     }
   }
 }
```

#### Queries

In contrast to commands which perform state change operations, queries are read-only operations which do not modify the
server-side state machine's state. Because read operations do not modify the state machine state, Copycat can optimize
queries according to read from certain nodes according to the configuration and may not require contacting a majority
of the cluster in order to maintain consistency. This means queries can be an order of magnitude faster, so it is strongly
recommended that all read-only operations be implemented as queries.

To create a query, simply implement the `Query` interface:

```java
public class Get<T> implements Query {

}
```

As with `Command`, `Query` extends the base `Operation` interface which is `Serializable`. However, for the best performance
users should implement [Writable](#writable) or register an [ObjectWriter](#objectwriter).

Queries are applied to the server-side state machine in the same manner as commands, using the `@Apply` annotation:

```java
public class ValueStateMachine extends StateMachine {
  private Object value;

  /**
   * Returns the value.
   */
  @Query(Get.class)
  public Object get() {
    return value;
  }
}
```

Once the server-side state machine has been modified to handle the application of a query, again, the `Resource` implementation
must be modified to handle submitting the query to the Copycat cluster:

```java
@Stateful(ValueStateMachine.class)
public class Value<T> extends AbstractResource {

  /**
   * Gets the value of the resource.
   */
  public CompletableFuture<T> get() {
    return submit(new Get<>(value));
  }
}
```

### Filtering

TODO

## Serialization

Copycat provides an efficient custom serialization framework that's designed to operate on both disk and memory via a
common [Buffer](#buffers) abstraction. Users can use the serialization framework to heavily optimize the transport of
user-defined objects over the wire and reduce the use of disk space and memory in Copycat's logs.

Copycat's serializer can be used by simply instantiating a `Serializer` instance:

```java
Serializer serializer = new Serializer();

Person person = new Person(1234, "Jordan", "Halterman");

Buffer buffer = serializer.writeObject(person).flip();

Person result = serializer.readObject(buffer);
```

The `Serializer` class supports serialization and deserialization of `Writable` types, types that have an associated
`ObjectWriter`, and native Java `Serializable` types, with `Serializable` being the most inefficient method of
serialization.

### Registering serializers

Serializers are registered with the Copycat `Serializer` via annotations. When a Copycat client or server is started,
Copycat scans the classpath for classes that implement [ObjectWriter](#objectwriter) or [Writable](#writable). At some
startup cost, this allows serializers (`ObjectWriter` implementations) to be associated with the types they serialize
via generic types and annotations rather than storing metadata in separate files.

### Buffers

At the core of Copycat's serialization framework and log is the `Buffer`. One might wonder why we need another buffer
abstraction when we have `ByteBuffer` and Netty's `ByteBuf`. Copycat's buffer abstraction was designed to operate both
on disk and in memory. This allows Copycat to serialize and deserialize objects directly to and from disk rather than
loading raw bytes into an intermediate buffer during deserialization. The buffer abstraction also allows Copycat to
easily alter the persistence level for its underlying logs without affecting the higher level log implementation.

### ObjectWriter

At the core of the serialization framework is the `ObjectWriter`. The `ObjectWriter` is a simple interface that exposes
two methods for serializing and deserializing objects of a specific type respectively. That is, object writers are responsible
for serializing objects of other types, and not themselves. Copycat provides this separate serialization class in order
to allow users to create custom serializers for types that couldn't otherwise be serialized by Copycat's `Serializer`.

The `ObjectWriter` interface consists of two methods:

```java
public class FooWriter implements ObjectWriter<Foo> {

  @Override
  public void write(Foo foo, Buffer buffer, Serializer serializer) {
    buffer.writeInt(foo.getBar());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Foo read(Class<Foo> type, Buffer buffer, Serializer serializer) {
    Foo foo = new Foo();
    foo.setBar(buffer.readInt());
  }
}
```

To serialize and deserialize an object, we simply write to and read from the passed in `Buffer` instance. In addition to
the `Buffer`, the `Serializer` that is serializing or deserializing the instance is also passed in. This allows the
serializer to serialize or deserialize subtypes as well:

```java
public class FooWriter implements ObjectWriter<Foo> {

  @Override
  public void write(Foo foo, Buffer buffer, Serializer serializer) {
    buffer.writeInt(foo.getBar());
    Baz baz = foo.getBaz();
    serializer.writeObject(baz, buffer);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Foo read(Class<Foo> type, Buffer buffer, Serializer serializer) {
    Foo foo = new Foo();
    foo.setBar(buffer.readInt());
    foo.setBaz(serializer.readObject(buffer));
  }
}
```

Copycat comes with a number of native `ObjectWriter` implementations, for instance `ListWriter`:

```java
@Serialize(@Serialize.Type(id=9, type=List.class))
public class ListWriter implements ObjectWriter<List> {

  @Override
  public void write(List object, Buffer buffer, Serializer serializer) {
    buffer.writeUnsignedShort(object.size());
    for (Object value : object) {
      serializer.writeObject(value, buffer);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List read(Class<List> type, Buffer buffer, Serializer serializer) {
    int size = buffer.readUnsignedShort();
    List object = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      object.add(serializer.readObject(buffer));
    }
    return object;
  }

}
```

Note that the `ListWriter` class is also annotated with the `@Serialize` annotation. This annotation specifies the type
that the writer serializes. The `@Serialize` annotation is, however, optional, as Copycat can determine the serializable
type by the generic type as well.

But there is another very important function that the `@Serialize` annotation provides: a serializable type `id`. For types
that define a type `id`, Copycat will serialize objects of that type along with the type `id` rather than the class name.
This significantly reduces the size of a serialized object by passing around a 16-bit unsigned integer rather than an
arbitrary string. For types that don't define a type `id`, Copycat will serialize the fully qualified class name for
reference during deserialization.

Alternatively, serializable types can indicate the `ObjectWriter` with which to serialize the type:

```java
@SerializeWith(id=100, serializer=FooWriter.class)
public class Foo {
  ...
}
```

### Writable

Instead of writing a custom `ObjectWriter`, serializable types can also implement the `Writable` interface. The `Writable`
interface is synonymous with Java's native `Serializable` interface. As with the `ObjectWriter` interface, `Writable`
exposes two methods which receive both a `Buffer` and a `Serializer`:

```java
public class Foo implements Writable {
  private int bar;
  private Baz baz;

  public Foo() {
  }

  public Foo(int bar, Baz baz) {
    this.bar = bar;
    this.baz = baz;
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeInt(bar);
    serializer.writeObject(baz);
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    bar = buffer.readInt();
    baz = serializer.readObject(buffer);
  }
}
```

For the most efficient serialization, it is essential that you associate a serializable type `id` with all serializable
types. Type IDs can be provided for types that implement `Writable` via the `@SerializeWith` annotation:

```java
@SerializeWith(id=100)
public class Foo implements Writable {
  ...

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeInt(bar);
    serializer.writeObject(baz);
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    bar = buffer.readInt();
    baz = serializer.readObject(buffer);
  }
}
```

The serializable type `id` can be any number between `0` and `65535`.

### Reference counting

TODO

### [User Manual](#user-manual)
