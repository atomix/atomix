/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.protocols.raft.service;

import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.event.RaftEvent;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.time.LogicalTimestamp;
import io.atomix.time.WallClockTimestamp;

import java.util.function.Function;

/**
 * Represents the committed state and metadata of a Raft state machine operation.
 */
public interface Commit<T> {

  /**
   * Returns the commit index.
   * <p>
   * This is the index at which the committed {@link RaftOperation} was written in the Raft log.
   * Raft guarantees that this index will be unique for {@link RaftOperation} commits and will be the same for all
   * instances of the given operation on all servers in the cluster.
   * <p>
   * For {@link RaftOperation} operations, the returned {@code index} may actually be representative of the last committed
   * index in the Raft log since queries are not actually written to disk. Thus, query commits cannot be assumed
   * to have unique indexes.
   *
   * @return The commit index.
   */
  long index();

  /**
   * Returns the session that submitted the operation.
   * <p>
   * The returned {@link RaftSession} is representative of the session that submitted the operation
   * that resulted in this {@link Commit}. The session can be used to {@link RaftSession#publish(RaftEvent)}
   * event messages to the client.
   *
   * @return The session that created the commit.
   */
  RaftSession session();

  /**
   * Returns the logical time at which the operation was committed.
   *
   * @return The logical commit time.
   */
  LogicalTimestamp logicalTime();

  /**
   * Returns the time at which the operation was committed.
   * <p>
   * The time is representative of the time at which the leader wrote the operation to its log. Because instants
   * are replicated through the Raft consensus algorithm, they are guaranteed to be consistent across all servers
   * and therefore can be used to perform time-dependent operations such as expiring keys or timeouts. Additionally,
   * commit times are guaranteed to progress monotonically, never going back in time.
   * <p>
   * Users should <em>never</em> use {@code System} time to control behavior in a state machine and should instead rely
   * upon {@link Commit} times or use the {@link RaftServiceExecutor} for time-based controls.
   *
   * @return The commit time.
   */
  WallClockTimestamp wallClockTime();

  /**
   * Returns the operation identifier.
   *
   * @return the operation identifier
   */
  OperationId operation();

  /**
   * Returns the operation submitted by the client.
   *
   * @return The operation submitted by the client.
   */
  T value();

  /**
   * Converts the commit from one type to another.
   *
   * @param transcoder the transcoder with which to transcode the commit value
   * @param <U> the output commit value type
   * @return the mapped commit
   */
  <U> Commit<U> map(Function<T, U> transcoder);

  /**
   * Converts the commit to a null valued commit.
   *
   * @return the mapped commit
   */
  Commit<Void> mapToNull();

}
