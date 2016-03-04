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

#### [Group membership](http://atomix.io/atomix/docs/coordination/#distributedgroup)

##### Cluster management

Track the nodes in a cluster:
```java
// Get a distributed membership group
DistributedGroup group = atomix.getGroup("my-group").get();

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

##### Leader election

Elect a leader among all members in the group:
```java
DistributedGroup group = atomix.getGroup("leader-election").get();

// Register an election listener on the group
group.onElection(member -> {
  System.out.println(member.id() + " elected leader!");
});

// Join the group
group.join();
```

##### Direct messaging

Send a direct message to a member of the group:
```java
DistributedGroup.Options options = new DistributedGroup.Options()
  .withAddress(new Address("localhost", 6000));

DistributedGroup group = atomix.getGroup("message-group", options).get();

// Join the group
group.join("member-1").thenAccept(member -> {

  // Register a message listener for the "greetings" topic
  member.connection().onMessage("greetings", message -> {
    message.reply("world");
  });

});
```

```java
DistributedGroup group = atomix.getGroup("message-group").get();

// Send a message to the "greetings" topic of member-1 and react when a response is received
group.member("member-1").connection().send("greetings", "Hello").thenAccept(reply -> {
  System.out.println("Hello " + reply);
});
```

##### Reliable task queues

Submit a task to a persistent, replicated task queue to be processed by a member of the group:
```java
DistributedGroup group = atomix.getGroup("task-group").get();

// Join the group
group.join("member-1").thenAccept(member -> {

  // Register a task handler for the joined member
  member.tasks().onTask(task -> {
    // Process and acknowledge the task
    doProcessing(task);
    task.ack();
  });

});
```

```java
DistributedGroup group = atomix.getGroup("task-group").get();

// Submit a task to member-1
group.member("member-1").tasks().submit("doWork").thenRun(() -> {
  // The task has been acknowledged
  System.out.println("Task complete!");
});

// Submit a task to all the members of the group
group.tasks().submit("everyoneDoWork").thenRun(() -> {
  // The task has been acknowledged
  System.out.println("Task complete!");
});
```

##### Cluster partitioning

Submit tasks to 3 members using round-robin:
```java
// Create a group configuration with three partitions
DistributedGroup.Config config = new DistributedGroup.Config()
  .withPartitioner(RoundRobinPartitioner.class)
  .withPartitions(3);

DistributedGroup group = atomix.getGroup("partition-group", config).get();

// Iterate through the partitions in the group
group.partitions().forEach(partition -> {
  // For each partition, submit a task to the first partition member
  partition.member(0).tasks().submit("doWork").thenRun(() -> {
    System.out.println("Work complete!");
  });
});
```

Send messages to three replicas using consistent hashing
```java
// Create a group configuration with three partitions
DistributedGroup.Config config = new DistributedGroup.Config()
  .withPartitioner(HashPartitioner.class)
  .withPartitions(32)
  .withVirtualNodes(100)
  .withReplicationFactor(3);

DistributedGroup group = atomix.getGroup("partition-group", config).get();

String[] values = new String[]{"foo", "bar", "baz"};

// Iterate through the values to send
for (String value : values) {
  // Get the partition for the value
  GroupPartition partition = group.partitions().partition(value);
  
  // Send a put message to each member in the partition
  CompletableFuture[] futures = new CompletableFuture[partition.members().size()];
  for (int i = 0; i < partition.members().size(); i++) {
    futures[i] = partition.member(i).connection().send("put", value);
  }
  
  // Call a callback once the message has been acknowledged by all members and print a message
  CompletableFuture.allOf(futures).thenRun(() -> {
    System.out.println("Replicated value: " + value);
  });
}
```

##### Remote execution

```java
DistributedGroup group = atomix.getGroup("execution-group").get();

// Execute a callback on member-1 in 10 seconds
group.member("member-1").scheduler().schedule(Duration.ofSeconds(10), (Runnable & Serialiable) () -> {
  System.out.println("I am member-1!");
});
```

#### [Locks](http://atomix.io/atomix/docs/coordination/#distributedlock)
```java
// Get a distributed lock
DistributedLock lock = atomix.getLock("my-lock").get();

// Acquire the lock
CompletableFuture<Void> future = lock.lock();

// Once the lock is acquired, release the lock
future.thenRun(() -> lock.unlock());
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
