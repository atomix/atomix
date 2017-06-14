/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Standalone segmented log for Copycat's <a href="https://raftconsensus.github.io/">Raft</a> implementation.
 * <p>
 * Logs are the vehicle through which Copycat servers persist and replicate state changes. The Copycat log is designed
 * specifically for use with the Raft consensus algorithm. The log is partitioned into multiple files called
 * <em>segments</em>. Each segment represents a sequence of indexes in the log. As entries are written to the log and
 * segments fill up, the log rolls over to new segments. Once a completed segment has been written and the entries
 * within it have been committed, the segment is compacted.
 * <p>
 * Log compaction is a two-stage process. The {@link io.atomix.copycat.server.storage.compaction.MinorCompactionTask minor compaction}
 * process periodically rewrites segments to remove non-tombstone and snapshot-related entries that have been released by the
 * state machine. The {@link io.atomix.copycat.server.storage.compaction.MajorCompactionTask major compaction} process periodically
 * rewrites segments to remove tombstones and combines multiple segments together to reduce the number of open file descriptors.
 * <p>
 * Copycat logs also support {@link io.atomix.copycat.server.storage.snapshot.SnapshotStore snapshotting}. Each snapshot
 * taken of the state machine's state is associated with a number of snapshotted entries. When segments are compacted,
 * entries compacted by the last snapshot are removed from segment files on disk.
 * <p>
 * For more information on Copycat's log and log compaction algorithms, see the
 * <a href="http://atomix.io/copycat/docs/internals/#the-copycat-log">log documentation on the Atomix website</a>.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix.protocols.raft.server.storage;
