[Atomix][Website]
=======

**Persistent • Consistent • Fault-tolerant • Database • Coordination • Framework**

#### [Getting started][Getting started] • [User Manual][User manual] • [Raft Algorithm][Copycat] • [Google Group][Google group] • [Javadoc][Javadoc]

[![Build Status](https://travis-ci.org/atomix/atomix.png)](https://travis-ci.org/atomix/atomix)

Atomix is a high-level asynchronous framework for building fault-tolerant distributed systems. It combines the consistency of
[ZooKeeper](https://zookeeper.apache.org/) with the usability of [Hazelcast](http://hazelcast.org/) to provide tools for managing
and coordinating stateful resources in a distributed system. Its strongly consistent, fault-tolerant data store is designed for
such use cases as configuration management, service discovery, group membership, scheduling, messaging, and synchronizing distributed
processes.

Atomix exposes a set of high level APIs with tools to solve a variety of distributed systems problems including:
* [Distributed coordination tools](http://atomix.io/atomix/user-manual/coordination/)
* [Distributed collections](http://atomix.io/atomix/user-manual/collections/)
* [Distributed atomic variables](http://atomix.io/atomix/user-manual/atomics/)

Additionally, Atomix is built on a series of low-level libraries that form its consensus algorithm:
* [Copycat][Copycat] is Atomix's progressive, feature-complete implementation of the [Raft consensus algorithm][Raft]
* [Catalyst][Catalyst] is Atomix's extensible asynchronous I/O, serialization, and networking framework

**Atomix is still undergoing heavy development and testing and is therefore not recommended for production!**

[Jepsen](https://github.com/aphyr/jepsen) tests are [currently being developed](http://github.com/jhalterman/atomix-jepsen)
to verify the stability of Atomix in an unreliable distributed environment. There is still work to be done, and Atomix
will not be fully released until significant testing is done both via normal testing frameworks and Jepsen. In the meantime,
Atomix snapshots will be pushed, and a beta release of Atomix is expected within the coming weeks. Follow the project for
updates!

#### [Website][Website] • [User Manual][User manual] • [Raft Algorithm][Copycat] • [Google Group][Google group] • [Javadoc][Javadoc]

[Website]: http://atomix.github.io/atomix/
[Getting started]: http://atomix.io/atomix/getting-started/
[User manual]: http://atomix.io/atomix/user-manual/
[Google group]: https://groups.google.com/forum/#!forum/copycat
[Javadoc]: http://atomix.io/atomix/api/latest/
[Raft]: https://raft.github.io/
[Copycat]: http://github.com/atomix/copycat
[Catalyst]: http://github.com/atomix/catalyst
