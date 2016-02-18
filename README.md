# [Atomix][Website]

[![Build Status](https://travis-ci.org/atomix/atomix.svg)](https://travis-ci.org/atomix/atomix)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.atomix/atomix/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.atomix/atomix)
[![Gitter](https://img.shields.io/badge/GITTER-join%20chat-green.svg)](https://gitter.im/atomix/atomix)


**Persistent • Consistent • Fault-tolerant • Asynchronous • Database • Coordination • Framework**

##### [Getting started][Getting started] • [User Manual][User manual] • [Javadoc][Javadoc] • [Raft Algorithm][Copycat] • [Jepsen Tests](https://github.com/atomix/atomix-jepsen) • [Google Group][Google group]

Atomix is a high-level asynchronous framework for building fault-tolerant distributed systems. It combines the consistency of [ZooKeeper](https://zookeeper.apache.org/) with the usability of [Hazelcast](http://hazelcast.org/) to provide tools for managing and coordinating stateful resources in a distributed system. Its strongly consistent, fault-tolerant data store is designed for such use cases as:

* [Configuration management](http://atomix.io/atomix/docs/collections/)
* [Service discovery](http://atomix.io/atomix/docs/coordination/#distributedgroup)
* [Group membership](http://atomix.io/atomix/docs/coordination/#distributedgroup)
* [Leader election](http://atomix.io/atomix/docs/coordination/#leader-election)
* [Scheduling](http://atomix.io/atomix/docs/coordination/#scheduling-remote-callbacks)
* [Messaging](http://atomix.io/atomix/docs/messaging/)
* [Synchronization](http://atomix.io/atomix/docs/coordination/#distributedlock)

#### [Locks](http://atomix.io/atomix/docs/coordination/#distributedlock)
```java
// Get a distributed lock
DistributedLock lock = atomix.getLock("my-lock").get();

// Acquire the lock
CompletableFuture<Void> future = lock.lock();

// Once the lock is acquired, release the lock
future.thenRun(() -> lock.unlock());
```

#### [Group membership](http://atomix.io/atomix/docs/coordination/#distributedgroup)
```java
// Get a distributed membership group
DistributedMembershipGroup group = atomix.getMembershipGroup("my-group").get();

// Join the group
group.join().thenAccept(member -> {

  // Leave the group
  member.leave();

});

// When a member joins the group, print a message
group.onJoin(member -> System.out.println(member.id() + " joined!"));

// When a member leaves the group, print a message
group.onLeave(member -> System.out.println(member.id() + " left!"));
```

#### [Leader election](http://atomix.io/atomix/docs/coordination/#leader-election)
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

#### [Messaging](http://atomix.io/atomix/docs/messaging/)
```java
// Get a distributed topic
DistributedTopic<String> topic = atomix.getTopic("my-topic");

// Register a message consumer
topic.consumer(message -> System.out.println(message));

// Publish a message to the topic
topic.publish("Hello world!");
```

#### [Variables](http://atomix.io/atomix/docs/variables/)
```java
// Get a distributed long
DistributedLong counter = atomix.getLong("my-long").get();

// Increment the counter
long value = counter.incrementAndGet().get();
```

#### [Collections](http://atomix.io/atomix/docs/collections/)
```java
// Get a distributed map
DistributedMap<String, String> map = atomix.getMap("my-map").get();

// Put a value in the map
map.put("atomix", "is great!").join();

// Get a value from the map
map.get("atomix").thenAccept(value -> System.out.println("atomix " + value));
```

...[and much more][Website]

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

[Website]: http://atomix.io/atomix/
[Getting started]: http://atomix.io/atomix/docs/getting-started/
[User manual]: http://atomix.io/atomix/docs/
[Google group]: https://groups.google.com/forum/#!forum/atomixio
[Javadoc]: http://atomix.io/atomix/api/latest/
[Raft]: https://raft.github.io/
[Copycat]: http://github.com/atomix/copycat
[Catalyst]: http://github.com/atomix/catalyst
