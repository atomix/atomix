# [Atomix][Website]

[![Build Status](https://travis-ci.org/atomix/atomix.svg)](https://travis-ci.org/atomix/atomix)
[![Coverage Status](https://coveralls.io/repos/github/atomix/atomix/badge.svg?branch=master)](https://coveralls.io/github/atomix/atomix?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.atomix/atomix/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.atomix/atomix)
[![Gitter](https://img.shields.io/badge/GITTER-join%20chat-green.svg)](https://gitter.im/atomix/atomix)

**An advanced platform for building fault-tolerant distributed systems on the JVM**

### Atomix 2.x

*Atomix 2.x documentation is currently under development. Check the website for updates!*

Atomix 2.x has been completely rewritten for better performance, scalability, and usability.
* [Cluster management](https://github.com/atomix/atomix/tree/master/cluster)
* [Cluster communication](https://github.com/atomix/atomix/tree/master/cluster)
* [Mature Raft implementation](https://github.com/atomix/atomix/tree/master/protocols/raft)
  * Pre-vote election protocol (§[4.2.3][dissertation])
  * Session-based linearizable writes (§[6.3][dissertation])
  * Lease-based fast linearizable reads from leaders (§[6.4.1][dissertation])
  * Fast sequential reads from followers (§[6.4.1][dissertation])
  * FIFO consistency for concurrent/asynchronous operations (§[11.1.2][dissertation])
  * Session-based state machine events (§[6.3][dissertation])
  * Membership changes (§[4.3][dissertation])
  * Snapshots (§[5.1][dissertation])
  * Phi accrual failure detection
* [Distributed systems primitives](https://github.com/atomix/atomix/tree/master/primitives)
  * [AtomicCounter](https://github.com/atomix/atomix/tree/master/primitives/src/main/java/io/atomix/primitives/counter)
  * [AtomicIdGenerator](https://github.com/atomix/atomix/tree/master/primitives/src/main/java/io/atomix/primitives/generator)
  * [LeaderElector](https://github.com/atomix/atomix/tree/master/primitives/src/main/java/io/atomix/primitives/leadership)
  * [DistributedLock](https://github.com/atomix/atomix/tree/master/primitives/src/main/java/io/atomix/primitives/lock)
  * [ConsistentMap](https://github.com/atomix/atomix/tree/master/primitives/src/main/java/io/atomix/primitives/map)
  * [ConsistentTreeMap](https://github.com/atomix/atomix/tree/master/primitives/src/main/java/io/atomix/primitives/map)
  * [AtomicCounterMap](https://github.com/atomix/atomix/tree/master/primitives/src/main/java/io/atomix/primitives/map)
  * [ConsistentMultimap](https://github.com/atomix/atomix/tree/master/primitives/src/main/java/io/atomix/primitives/multimap)
  * [WorkQueue](https://github.com/atomix/atomix/tree/master/primitives/src/main/java/io/atomix/primitives/queue)
  * [DistributedSet](https://github.com/atomix/atomix/tree/master/primitives/src/main/java/io/atomix/primitives/set)
  * [DocumentTree](https://github.com/atomix/atomix/tree/master/primitives/src/main/java/io/atomix/primitives/tree)
  * [AtomicValue](https://github.com/atomix/atomix/tree/master/primitives/src/main/java/io/atomix/primitives/value)
* [Partitioning](https://github.com/atomix/atomix/tree/master/partition)
* [REST API](https://github.com/atomix/atomix/tree/master/rest)
* [Interactive CLI](https://github.com/atomix/atomix/tree/master/cli)

### Java API

#### Create a node
```java
Atomix atomix = Atomix.builder()
  .withLocalNode(Node.builder()
    .withId(NodeId.from("a"))
    .withEndpoint(Endpoint.from("localhost", 5678))
    .build())
  .withBootstrapNodes(bootstrap) // A list of `Node`s that replicate state
  .build()
  .open()
  .join();
```

#### Get a list of nodes in the cluster
```java
Collection<Node> nodes = atomix.getClusterService().getNodes();
```

#### Send a direct message
```java
MessageSubject subject = new MessageSubject("message");
atomix.getCommuncationService().unicast(subject, "Hello world!", NodeId.from("b"));
```

#### Receive messages
```java
atomix.getCommunicationService().addSubscriber(subject, message -> {
  System.out.println("Received " + message);
});
```

#### Send an event
```java
atomix.getEventService().unicast(subject, "Hello world!");
```

#### Subscribe to receive events
```java
// Subscribe to events
atomix.getEventService().addSubscriber(subject, message -> {
  System.out.println("Received " + message);
});
```

#### Synchronous distributed lock
```java
DistributedLock lock = atomix.lockBuilder()
  .withName("my-lock")
  .build();

lock.lock();
try {
  // Do stuff...
} finally {
  lock.unlock();
}
```

#### Asynchronous distributed lock
```java
AsyncDistributedLock lock = atomix.lockBuilder()
  .withName("my-lock")
  .buildAsync();

lock.lock().thenAccept(version -> {
  // Do stuff...
  lock.unlock();
});
```

### HTTP API

#### Run a standalone server
```
mvn clean package
bin/atomix server
```

#### Start a cluster
```
bin/atomix server a:localhost:5000 --bootstrap a:localhost:5000 b:localhost:5001 c:localhost:5002 --http-port 6000 --data-dir data/a
```

```
bin/atomix server b:localhost:5001 --bootstrap a:localhost:5000 b:localhost:5001 c:localhost:5002 --http-port 6001 --data-dir data/b
```

```
bin/atomix server c:localhost:5002 --bootstrap a:localhost:5000 b:localhost:5001 c:localhost:5002 --http-port 6002 --data-dir data/c
```

#### Start a client node
```
bin/atomix server --bootstrap a:localhost:5000 b:localhost:5001 c:localhost:5002
```

#### Acquire a lock
```
curl -XPOST http://localhost:5678/v1/primitives/locks/my-lock
```

#### Release a lock
```
curl -XDELETE http://localhost:5678/v1/primitives/locks/my-lock
```

#### Set a value in a map
```
curl -XPUT http://localhost:5678/v1/primitives/maps/my-map/foo -d value="Hello world!" -H "Content-Type: text/plain"
```

#### Get a value in a map
```
curl -XGET http://localhost:5678/v1/primitives/maps/my-map/foo
```

#### Send an event
```
curl -XPOST http://localhost:5678/v1/events/something-happened -d "Something happened!" -H "Content-Type: text/plain"
```

#### Receive events
```
curl -XGET http://localhost:5678/v1/events/something-happened
```

### Docker
```
mvn clean package
docker build -t atomix .
docker run -p 5678:5678 -p 5679:5679 atomix docker:0.0.0.0
```

### Command line interface

```
bin/atomix
```

#### Print help

```
atomix> help
cluster:
    cluster node [id]
    cluster nodes
counter:
    counter {counter} get
    counter {counter} set {value}
    counter {counter} increment
election:
    election {election} run
    election {election} leader
    election {election} listen {candidate}
    election {election} anoint {candidate}
    election {election} evict {candidate}
    election {election} promote {candidate}
    election {election} withdraw {candidate}
events:
    events {topic} publish {text}
    events {topic} listen
    events {topic} subscribe
    events {topic} consume {subscriber}
    events {topic} unsubscribe {subscriber}
exit:
    exit
help:
    help [command]
id:
    id {generator} next
lock:
    lock {id} lock
    lock {id} unlock
map:
    map {map} get {key}
    map {map} put {key} {text}
    map {map} remove {key}
    map {map} size
    map {map} clear
messages:
    messages {subject} publish {text}
    messages {subject} send {node} {text}
    messages {subject} listen
    messages {subject} subscribe
    messages {subject} consume {subscriber}
    messages {subject} unsubscribe {subscriber}
queue:
    queue {queue} add {item}
    queue {queue} take
    queue {queue} consume
set:
    set {set} add {text}
    set {set} remove {text}
    set {set} contains {text}
    set {set} size
    set {set} clear
tree:
    tree {tree} create {path} {text}
    tree {tree} set {path} {text}
    tree {tree} get {path}
    tree {tree} replace {path} {text} {version}
value:
    value {value} get
    value {value} set {text}
    value {value} compare-and-set {expect} {update}
```

#### Print command help
```
atomix> election foo leader
{
  "candidates": [
    "d718897a-e651-416c-94c3-9ddc09b2d0e5",
    "7f01be7d-514f-4561-a2ba-3ccbf3edfbb2"
  ],
  "leader": "d718897a-e651-416c-94c3-9ddc09b2d0e5"
}
```

#### Run a command
```
election foo leader
```

### Acknowledgements

Thank you to the [Open Networking Foundation][ONF] and [ONOS][ONOS]
for continued support of Atomix!

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of [YourKit Java Profiler](https://www.yourkit.com/java/profiler/)
and [YourKit .NET Profiler](https://www.yourkit.com/.net/profiler/),
innovative and intelligent tools for profiling Java and .NET applications.

![YourKit](https://www.yourkit.com/images/yklogo.png)

[Website]: http://atomix.io/atomix/
[Getting started]: http://atomix.io/atomix/docs/getting-started/
[User manual]: http://atomix.io/atomix/docs/
[Google group]: https://groups.google.com/forum/#!forum/atomixio
[Javadoc]: http://atomix.io/atomix/api/latest/
[Raft]: https://raft.github.io/
[ONF]: https://www.opennetworking.org/
[ONOS]: http://onosproject.org/
[Copycat]: https://github.com/atomix/copycat
[Cluster management]: https://github.com/atomix/atomix/tree/master/core/src/main/java/io/atomix/cluster
[Messaging]: https://github.com/atomix/atomix/tree/master/core/src/main/java/io/atomix/cluster/messaging
[Partitioning]: https://github.com/atomix/atomix/tree/master/core/src/main/java/io/atomix/partition
[Distributed systems primitives]: https://github.com/atomix/atomix/tree/master/core/src/main/java/io/atomix/primitives
[Transactions]: https://github.com/atomix/atomix/tree/master/core/src/main/java/io/atomix/transaction
[Raft protocol]: https://github.com/atomix/atomix/tree/master/protocols/raft
[Gossip protocol]: https://github.com/atomix/atomix/tree/master/protocols/gossip
[Failure detection protocol]: https://github.com/atomix/atomix/tree/master/protocols/failure-detection
[Primary-backup protocol]: https://github.com/atomix/atomix/tree/master/protocols/backup
[Time protocols]: https://github.com/atomix/atomix/tree/master/time
[Protocols]: https://github.com/atomix/atomix/tree/master/protocols
