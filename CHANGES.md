# Change Log
All notable changes to this project will be documented in this file.

## Unreleased

## 1.0.0 - 2017-01-12

### Bug Fixes
* [#213](https://github.com/atomix/atomix/pull/213) - Ensure producer message ID is incremented for asynchronous group messages
* [#197](https://github.com/atomix/atomix/pull/197) - Recover client-side resource state on session failure
* [#195](https://github.com/atomix/atomix/pull/195) - Initialize `DistributedGroup` members on startup
* [#190](https://github.com/atomix/atomix/pull/190) - Automatically recover group members on session failure

### Features
* [#221](https://github.com/atomix/atomix/pull/221) - Support client-side `DistributedMap` caching
* [#217](https://github.com/atomix/atomix/pull/200) - Support status changes in group members and ensure persistent members are never removed from the group
* [#214](https://github.com/atomix/atomix/pull/214) - Add generic event API for resources and events for `DistributedValue`, `DistributedLong`, `DistributedMap`, `DistributedSet`, and `DistributedQueue`
* [#200](https://github.com/atomix/atomix/pull/200) - Support collection iterators
