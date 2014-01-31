CopyCat
=======

CopyCat is a replication framework for Vert.x. It provides for state machine replication
using the Raft consensus algorithm as described in
[this paper](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

**This project is still very much under development and is recommended for testing only**

#### Features
* Automatically replicates state across multiple Vert.x instances in a consistent and reliable manner
* Supports runtime cluster membership changes
* Supports replicated log persistence in MongoDB, Redis, or in memory
* Provides an API for periodic log cleaning
* Provides tools for automatically detecting cluster members
* Exposes an event bus API for executing state machine commands remotely
* Uses adaptive failure detection for fast leader re-election
