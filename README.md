# [Atomix][Website]

[![Build Status](https://travis-ci.org/atomix/atomix.svg)](https://travis-ci.org/atomix/atomix)
[![Coverage Status](https://coveralls.io/repos/github/atomix/atomix/badge.svg?branch=master)](https://coveralls.io/github/atomix/atomix?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.atomix/atomix/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.atomix/atomix)
[![Gitter](https://img.shields.io/badge/GITTER-join%20chat-green.svg)](https://gitter.im/atomix/atomix)

**An advanced platform for building fault-tolerant distributed systems on the JVM**

## Atomix 2.0 is coming!

The current _master_ branch is the current working branch for Atomix 2.0 development. The new release is the
culmination of years of experience working with and building on the protocols in Atomix and Copycat to create
more scalable, dynamic framework for building fault-tolerant distributed systems.

The core of Atomix 2.0 features the following high-level abstractions:
* [Cluster management][Cluster management]
* [Cluster communication][Messaging]
  * Point-to-point messaging
  * Publish-subscribe messaging
* [Scalable, persistent, consistent distributed systems primitives][Distributed systems primitives]
  * Leader election
  * Locks
  * Maps
  * Sets
  * Trees
  * Counters
  * Global ID generators
  * Work queues
  * Transactions
  * Custom primitives
  * etc
* [Scalable, ephemeral, eventually consistent primitives][Distributed systems primitives]
  * Maps
  * Sets
* [Transactions][Transactions]

Underlying these abstractions is a variety of generic implementations of common distributed systems
[protocols and algorithms][Protocols]:
* [Raft consensus protocol][Raft protocol] (formerly [Copycat][Copycat])
* [Partitioning][Partitioning] (partitioned Raft clusters)
* [Failure detection][Failure detection protocol] (phi acrrual failure detectors)
* [Gossip][Gossip protocol]
* [Anti-entropy][Gossip protocol]
* [Primary-backup replication][Primary-backup protocol]
* [Time protocols][Time protocols] (logical clocks, vector clocks, etc)
* etc

[Copycat][Copycat] has also undergone significant refactoring for better performance, scalability, and
fault tolerance in Atomix 2.0. It can now be found in the [Raft][Raft protocol] module.

Atomix 2.0 is targeted for release in fall 2017

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