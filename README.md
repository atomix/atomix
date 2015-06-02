Copycat
=======

[![Build Status](https://travis-ci.org/kuujo/copycat.png)](https://travis-ci.org/kuujo/copycat)

Copycat is an extensible log-based distributed coordination framework for Java 8 built on the
[Raft consensus protocol](https://raftconsensus.github.io/).

#### [User Manual](#user-manual)
#### [Architecture](#architecture)

Copycat is a strongly consistent embedded distributed coordination built on the
[Raft consensus protocol](https://raftconsensus.github.io/). Copycat exposes a set of high level APIs with tools to
solve a variety of distributed systems problems including:
* [Replicated state machines](#state-machines)
* [Leader election](#leader-elections)
* [Distributed locks](#locks)
* [Distributed collections](#collections)
* [Distributed atomic variables](#atomic-variables)
* [Messaging](#messaging)
* [Remote execution](#remote-execution)

**Copycat is still undergoing heavy development. This branch is the current development branch and is still undergoing
testing and is therefore not recommended for production!**

**Copycat is now in a feature freeze for the 0.6 release.** This project will be released to Maven Central once is has
been significantly tested in code and via [Jepsen](https://github.com/aphyr/jepsen). Follow the project for updates!

Additionally, documentation is currently undergoing a complete rewrite due to the refactoring of significant portions
of the project.

*Copycat requires Java 8*

User Manual
===========

1. [Introduction](#introduction)
1. [Getting started](#getting-started)
   * [Concepts and terminology](#concepts-and-terminology)
   * [The builder pattern](#the-builder-pattern)
   * [Thread safety](#thread-safety)
1. [The Copycat API](#the-copycat-api)
   * [Configuration](#configuration)
   * [Creating a cluster](#creating-a-cluster)
1. [Resources](#resources)
   * [Creating resources](#creating-resources)
   * [Working with paths](#working-with-paths)
1. [Collections](#collections)
   * [AsyncMap](#asyncmap)
   * [AsyncList](#asynclist)
   * [AsyncSet](#asyncset)
   * [AsyncMultiMap](#asyncmultimap)
   * [AsyncLock](#asynclock)
1. [Locks](#locks)
   * [Creating a lock](#creating-a-lock)
1. [Leader elections](#leader-elections)
   * [Creating a leader election](#creating-a-leader-election)
1. [Atomic variables](#atomic-variables)
   * [AsyncLong](#asynclong)
   * [AsyncBoolean](#asyncboolean)
   * [AsyncReference](#asyncreference)
1. [Writing custom resources](#writing-custom-resources)
   * [Defining commands](#defining-commands)
   * [Defining queries](#defining-queries)
   * [Query consistency levels](#query-consistency-levels)
   *  [Custom state machines](#custom-state-machines)
      * [Creating a state machine](#creating-a-state-machine)
      * [Registering a state machine](#registering-a-state-machine)
      * [Applying commands and queries](#applying-commands-and-queries)
      * [Filtering commands from the log](#filtering-commands-from-the-log)
1. [Clusters](#clusters)
   * [Seed members](#seed-members)
   * [Member types](#member-types)
   * [Direct messaging](#direct-messaging)
   * [Broadcast messaging](#broadcast-messaging)
   * [Serialization](#serialization)
   * [Remote execution](#remote-execution)
1. [Serialization framework](#serialization-framework)
   * [Implementing Writable](#implementing-writable)
   * [Registering serializable types](#registering-serializable-types)
1. [Architecture](#architecture)
   * [Leader election](#leader-election)
   * [State machines](#state-machines)
   * [Logs](#logs)
   * [Sessions](#sessions)
1. [Contributing](#contributing)

## Introduction

Copycat is a fully asynchronous distributed coordination framework built on the [Raft consensus protocol](https://raftconsensus.github.io/).
Copycat facilitates numerous types of strongly consistent distributed data structures - referred to as
[resources](#resources) - based on a replicated log. Copycat exposes an embedded API that allows users to create arbitrary
named resources using a path-based system similar to that of [ZooKeeper](http://zookeeper.apache.org). However, in
contrast to ZooKeeper, Copycat provides high level distributed data structures and tools.

### [User Manual](#user-manual)
