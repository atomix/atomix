[Copycat][Website]
=======

**Persistent • Consistent • Fault-tolerant • Database • Coordination • Framework**

#### [Getting started][Getting started] • [User Manual][User manual] • [Raft Algorithm][Catalog] • [Google Group][Google group] • [Javadoc][Javadoc]

[![Build Status](https://travis-ci.org/atomix/copycat.png)](https://travis-ci.org/atomix/copycat)

Copycat is a high-level asynchronous framework for building fault-tolerant distributed systems. It combines the consistency of
[ZooKeeper](https://zookeeper.apache.org/) with the usability of [Hazelcast](http://hazelcast.org/) to provide tools for managing
and coordinating stateful resources in a distributed system. Its strongly consistent, fault-tolerant data store is designed for
such use cases as configuration management, service discovery, group membership, scheduling, messaging, and synchronizing distributed
processes.

Copycat exposes a set of high level APIs with tools to solve a variety of distributed systems problems including:
* [Distributed coordination tools](http://atomix.io/user-manual/distributed-resources/#distributed-coordination)
* [Distributed collections](http://atomix.io/user-manual/distributed-resources/#distributed-collections)
* [Distributed atomic variables](http://atomix.io/user-manual/distributed-resources/#distributed-atomic-variables)

Additionally, Copycat is built on a series of low-level libraries that form its consensus algorithm:
* [Catalog][Catalog] is Copycat's progressive, feature-complete implementation of the [Raft consensus algorithm][Raft]
* [Catalyst][Catalyst] is Copycat's extensible asynchronous I/O, serialization, and networking framework

**Copycat is still undergoing heavy development and testing and is therefore not recommended for production!**

[Jepsen](https://github.com/aphyr/jepsen) tests are [currently being developed](http://github.com/jhalterman/copycat-jepsen)
to verify the stability of Copycat in an unreliable distributed environment. There is still work to be done, and Copycat
will not be fully released until significant testing is done both via normal testing frameworks and Jepsen. In the meantime,
Copycat snapshots will be pushed, and a beta release of Copycat is expected within the coming weeks. Follow the project for
updates!

#### [Website][Website] • [User Manual][User manual] • [Raft Algorithm][Catalog] • [Google Group][Google group] • [Javadoc][Javadoc]

[Website]: http://atomix.github.io/
[Getting started]: http://atomix.io/getting-started/
[User manual]: http://atomix.io/user-manual/
[Google group]: https://groups.google.com/forum/#!forum/copycat
[Javadoc]: http://atomix.github.io/copycat/api/0.1.0/
[Raft]: https://raft.github.io/
[Catalog]: http://github.com/atomix/catalog
[Catalyst]: http://github.com/atomix/catalyst
