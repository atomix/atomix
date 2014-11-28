Copycat
=======

### [User Manual](#user-manual)

Copycat is an extensible Java-based implementation of the
[Raft consensus protocol](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

The core of Copycat is a framework designed to integrate with any asynchronous framework
or protocol. Copycat provides a simple extensible API that can be used to build a
strongly consistent, *fault/partition tolerant state machine* over any mode of communication.
Copycat's Raft implementation also supports advanced features of the Raft algorithm such as
snapshotting and dynamic cluster configuration changes and provides *additional optimizations
pipelining, fast Kryo-based serialization, memory-mapped file logging, and read-only state queries.*

Copycat is a pluggable framework, providing protocol and service implementations for
various frameworks such as [Netty](http://netty.io) and [Vert.x](http://vertx.io).

**Please note that Copycat is still undergoing heavy development, and until a beta release,
the API is subject to change.**

This project has been in development for over a year, and it's finally nearing its first release.
Until then, here are a few items on the TODO list:

* ~~Refactor the cluster/protocol APIs to provide more flexibility~~
* ~~Separate commands into separate Command (write) and Query (read) operations~~
* ~~Provide APIs for both synchronous and asynchronous environments~~
* Develop extensive integration tests for configuration, leader election and replication algorithms
* Update documentation for new API changes
* Publish to Maven Central!

Copycat *will* be published to Maven Central once these features are complete. Follow
the project for updates!

*Copycat requires Java 8. Though it has been requested, there are currently no imminent
plans for supporting Java 7. Of course, feel free to fork and PR :-)*

User Manual
===========

**Note: Some of this documentation may be inaccurate due to the rapid development currently
taking place on Copycat**
