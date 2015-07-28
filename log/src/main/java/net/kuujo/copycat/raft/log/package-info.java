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
 * Provides a standalone segmented log for Copycat's <a href="https://raftconsensus.github.io/">Raft</a> implementation.
 * <p>
 * The log is designed as a standalone journal built on Copycat's {@link net.kuujo.copycat.io.Buffer} abstraction.
 * The buffer abstraction allows Copycat's {@link net.kuujo.copycat.raft.log.Log} to write to memory or disk based on the
 * buffer type.
 * <p>
 * While the log is not dependent on the Raft algorithm, it does implement many features in support of the Raft implementation.
 * Specifically, the log is not an append-only log. Rather, it supports appending to, truncating, and compacting the
 * log.
 * <p>
 * The log achieves fast sequential writes by segmenting itself internally and compacting segments separately and
 * combining segments when necessary. Compaction works by iterating over a segment or segments,
 * {@link net.kuujo.copycat.raft.log.EntryFilter filtering} entries out of the segment, and rewriting the segment to a new
 * {@link net.kuujo.copycat.io.Buffer}. Compaction is done in two stages: major and minor. The minor compaction stage
 * relates to compacting a single segment. The log will prioritize minor compaction for more recent segments first.
 * Periodically, the log will execute a major compaction wherein all segments of the log are compacted together. This
 * means the entire log is traversed from start to finish, and segments are combined in cases where enough entries have
 * been removed.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package net.kuujo.copycat.raft.log;
