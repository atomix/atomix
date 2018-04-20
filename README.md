# [Atomix][Website]

![Atomix](http://atomix.io/assets/img/logos/atomix-medium.png)

[![Build Status](https://travis-ci.org/atomix/atomix.svg)](https://travis-ci.org/atomix/atomix)
[![Coverage Status](https://coveralls.io/repos/github/atomix/atomix/badge.svg?branch=master)](https://coveralls.io/github/atomix/atomix?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.atomix/atomix/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.atomix/atomix)
[![Gitter](https://img.shields.io/badge/GITTER-join%20chat-green.svg)](https://gitter.im/atomix/atomix)

**A reactive Java framework for building fault-tolerant distributed systems**

Please see the [website][Website] for full documentation.

Atomix 2.1 is a fully featured framework for building fault-tolerant distributed systems. It provides a set of high-level primitives commonly needed for building scalable and fault-tolerant distributed systems. These primitives include:
* Cluster management and failure detection
* Direct and publish-subscribe messaging
* Distributed coordination primitives built on a novel implementation of the [Raft][Raft] consensus protocol
* Scalable data primitives built on a multi-primary protocol
* Synchronous and asynchronous Java APIs
* Standalone agent
* REST API
* Interactive CLI

### Acknowledgements

Atomix is developed as part of the [ONOS][ONOS] project at the [Open Networking Foundation][ONF]. Atomix project thanks ONF for its ongoing support of this project.

![ONF](https://3vf60mmveq1g8vzn48q2o71a-wpengine.netdna-ssl.com/wp-content/uploads/2017/06/onf-logo.jpg)

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of [YourKit Java Profiler](https://www.yourkit.com/java/profiler/)
and [YourKit .NET Profiler](https://www.yourkit.com/.net/profiler/),
innovative and intelligent tools for profiling Java and .NET applications.

![YourKit](https://www.yourkit.com/images/yklogo.png)

[Website]: http://atomix.io
[Getting started]: http://atomix.io/docs/latest/getting-started/
[User manual]: http://atomix.io/docs/latest/user-manual/introduction/
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
