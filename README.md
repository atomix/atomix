[Atomix][Website]
=======

[![Build Status](https://travis-ci.org/atomix/atomix.png)](https://travis-ci.org/atomix/atomix)

**Persistent • Consistent • Fault-tolerant • Database • Coordination • Framework**

##### [Getting started][Getting started] • [User Manual][User manual] • [Javadoc][Javadoc] • [Raft Algorithm][Copycat] • [Jepsen Tests](https://github.com/atomix/atomix-jepsen) • [Google Group][Google group]

Atomix is a high-level asynchronous framework for building fault-tolerant distributed systems. It combines the consistency of
[ZooKeeper](https://zookeeper.apache.org/) with the usability of [Hazelcast](http://hazelcast.org/) to provide tools for managing
and coordinating stateful resources in a distributed system. Its strongly consistent, fault-tolerant data store is designed for
such use cases as configuration management, service discovery, group membership, scheduling, messaging, and synchronizing distributed
processes.

Atomix exposes a set of high level APIs with tools to solve a variety of distributed systems problems including:
* [Distributed coordination tools](http://atomix.io/atomix/user-manual/coordination/)
* [Distributed collections](http://atomix.io/atomix/user-manual/collections/)
* [Distributed atomic variables](http://atomix.io/atomix/user-manual/atomics/)

Atomix is built on [Copycat][Copycat], a well tested, [Jepsen](https://github.com/atomix/atomix-jepsen) verified
implementation of the [Raft consensus algorithm](https://raft.github.io/).

Early Jepsen testing of both Copycat and Atomix is now complete, and an early release of both projects will be
pushed to Maven Central in the coming days. In the meantime, snapshots are frequently pushed. Documentation is
still under development, and the website will continue to be updated until and after a release.

##### [Getting started][Getting started] • [User Manual][User manual] • [Javadoc][Javadoc] • [Raft Algorithm][Copycat] • [Jepsen Tests](https://github.com/atomix/atomix-jepsen) • [Google Group][Google group]

[Website]: http://atomix.github.io/atomix/
[Getting started]: http://atomix.io/atomix/getting-started/
[User manual]: http://atomix.io/atomix/user-manual/
[Google group]: https://groups.google.com/forum/#!forum/copycat
[Javadoc]: http://atomix.io/atomix/api/latest/
[Raft]: https://raft.github.io/
[Copycat]: http://github.com/atomix/copycat
[Catalyst]: http://github.com/atomix/catalyst
