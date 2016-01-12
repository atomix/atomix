# [Atomix][Website]

[![Build Status](https://travis-ci.org/atomix/atomix.svg)](https://travis-ci.org/atomix/atomix)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.atomix/atomix/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.atomix/atomix)
[![Gitter](https://img.shields.io/badge/GITTER-join%20chat-green.svg)](https://gitter.im/atomix/atomix)


**Persistent • Consistent • Fault-tolerant • Asynchronous • Database • Coordination • Framework**

##### [Getting started][Getting started] • [User Manual][User manual] • [Javadoc][Javadoc] • [Raft Algorithm][Copycat] • [Jepsen Tests](https://github.com/atomix/atomix-jepsen) • [Google Group][Google group]

Atomix is a high-level asynchronous framework for building fault-tolerant distributed systems. It combines the consistency of
[ZooKeeper](https://zookeeper.apache.org/) with the usability of [Hazelcast](http://hazelcast.org/) to provide tools for managing
and coordinating stateful resources in a distributed system. Its strongly consistent, fault-tolerant data store is designed for
such use cases as configuration management, service discovery, group membership, scheduling, messaging, and synchronizing distributed
processes.

*Note: the website and documentation is currently undergoing a rewrite for the 1.0 release. We realize many links are
broken and are actively working to update the documentation. Please reference the [Javadoc][Javadoc] for the most
up-to-date documentation.*

#### Locks
```java
// Get a distributed lock
DistributedLock lock = atomix.getLock("my-lock").get();

// Acquire the lock
CompletableFuture<Void> future = lock.lock();

// Once the lock is acquired, release the lock
future.thenRun(() -> lock.unlock());
```

See [distributed coordination](http://atomix.io/atomix/user-manual/coordination/)

#### Group membership
```java
// Get a distributed membership group
DistributedMembershipGroup group = atomix.getMembershipGroup("my-group").get();

// Join the group
group.join();

// When a member joins the group, print a message
group.onJoin(member -> System.out.println(member.id() + " joined!"));

// When a member leaves the group, print a message
group.onLeave(member -> System.out.println(member.id() + " left!"));
```

See [distributed coordination](http://atomix.io/atomix/user-manual/coordination/)

#### Leader election
```java
// Join a group
CompletableFuture<LocalMember> future = group.join();

// Once the member has joined the group, register an election listener
future.thenAccept(member -> {
  member.onElection(term -> {
    System.out.println("Elected leader!");
    member.resign();
  });
});
```

See [distributed coordination](http://atomix.io/atomix/user-manual/coordination/)

#### Messaging
```java
// Get a distributed topic
DistributedTopic<String> topic = atomix.getTopic("my-topic");

// Register a message consumer
topic.consumer(message -> System.out.println(message));

// Publish a message to the topic
topic.publish("Hello world!");
```

See [distributed messaging](http://atomix.io/atomix/user-manual/messaging/)

#### Variables
```java
// Get a distributed long
DistributedLong counter = atomix.getLong("my-long").get();

// Increment the counter
long value = counter.incrementAndGet().get();
```

See [distributed variables](http://atomix.io/atomix/user-manual/variables/)

#### Collections
```java
// Get a distributed map
DistributedMap<String, String> map = atomix.getMap("my-map").get();

// Put a value in the map
map.put("atomix", "is great!").join();

// Get a value from the map
map.get("atomix").thenAccept(value -> System.out.println("atomix " + value));
```

See [distributed collections](http://atomix.io/atomix/user-manual/collections/)

### Project status: BETA

Atomix is a fault-tolerant framework that provides strong consistency guarantees, and as such we take the responsibility
to test these claims and document the implementation very seriously. Atomix is built on [Copycat][Copycat], a well tested,
well documented, [Jepsen verified](https://github.com/atomix/atomix-jepsen) implementation of the
[Raft consensus algorithm](https://raft.github.io/). *But the beta label indicates that the implementation
may still have some bugs* or other issues that make it not quite suitable for production. Users are encouraged to
use Atomix in development and contribute to the increasing stability of the project with
[issues](https://github.com/atomix/copycat/issues) and [pull requests](https://github.com/atomix/copycat/pulls).
Once we've reached consensus on the lack of significant bugs in the beta release(s), a release candidate will be pushed.
Once we've reached consensus on the stability of the release candidate(s) and Atomix's production readiness, a full
release will be pushed.

**It's all about that consensus**!

Documentation for most of Atomix's implementation of the Raft algorithm is
[available on the Atomix website](http://atomix.github.io/copycat/user-manual/internals/), and users are encouraged
to [explore the Javadoc][Javadoc] which is also heavily documented. All documentation remains under continued
development, and websites for both Atomix and [Copycat][Copycat] will continue to be updated until and after a release.

### Examples

Users are encouraged to explore the examples in the `/examples` directory. Perhaps the most interesting/revelatory
example is the leader election example. This example demonstrates a set of replicas that elect a leader among themselves.

To run the leader election example:

1. Clone this repository: `git clone --branch master git@github.com:atomix/atomix.git`
1. Navigate to the project directory: `cd atomix`
1. Compile the project: `mvn package`
1. Run the following three commands in three separate processes from the same root directory of the project:

```
java -jar examples/leader-election/target/atomix-leader-election.jar logs/server1 localhost:5000 localhost:5001 localhost:5002
java -jar examples/leader-election/target/atomix-leader-election.jar logs/server2 localhost:5001 localhost:5000 localhost:5002
java -jar examples/leader-election/target/atomix-leader-election.jar logs/server3 localhost:5002 localhost:5000 localhost:5001
```

Each instance of the leader election example starts an [AtomixReplica](http://atomix.io/atomix/api/latest/io/atomix/AtomixReplica.html),
connects to the other replicas in the cluster, creates a [DistributedLeaderElection](http://atomix.io/atomix/api/latest/io/atomix/coordination/DistributedLeaderElection.html),
and awaits an election. The first time a node is elected leader it will print the message: `"Elected leader!"`. When one of
the processes is crashed, a new process will be elected a few seconds later and again print the message: `"Elected leader!"`.

Note that the same election process can be done with [AtomixClient](http://atomix.io/atomix/api/latest/io/atomix/AtomixClient.html)s as well. Atomix
provides the concept of stateful nodes (replicas) which store resource state changes on disk and replicate changes to other
replicas, and stateless nodes (clients) which operate on resources remotely. Both types of nodes can use the same resources
in the same ways. This makes Atomix particularly well suited for embedding in server-side technologies without the overhead
of a Raft server on every node.

See the [website][User manual] for documentation and examples.

##### [Getting started][Getting started] • [User Manual][User manual] • [Javadoc][Javadoc] • [Raft Algorithm][Copycat] • [Jepsen Tests](https://github.com/atomix/atomix-jepsen) • [Google Group][Google group]

[Website]: http://atomix.github.io/atomix/
[Getting started]: http://atomix.io/atomix/getting-started/
[User manual]: http://atomix.io/atomix/user-manual/
[Google group]: https://groups.google.com/forum/#!forum/copycat
[Javadoc]: http://atomix.io/atomix/api/latest/
[Raft]: https://raft.github.io/
[Copycat]: http://github.com/atomix/copycat
[Catalyst]: http://github.com/atomix/catalyst
