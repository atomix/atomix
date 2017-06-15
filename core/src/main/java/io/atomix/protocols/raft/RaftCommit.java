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
package io.atomix.protocols.raft;

import io.atomix.protocols.raft.session.RaftSession;

import java.time.Instant;

/**
 * Represents the committed state and metadata of a Raft state machine operation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RaftCommit<T extends RaftOperation> {

  /**
   * Returns the commit index.
   * <p>
   * This is the index at which the committed {@link Operation} was written in the Raft log.
   * Copycat guarantees that this index will be unique for {@link Command} commits and will be the same for all
   * instances of the given operation on all servers in the cluster.
   * <p>
   * For {@link Query} operations, the returned {@code index} may actually be representative of the last committed
   * index in the Raft log since queries are not actually written to disk. Thus, query commits cannot be assumed
   * to have unique indexes.
   *
   * @return The commit index.
   */
  long index();

  /**
   * Returns the session that submitted the operation.
   * <p>
   * The returned {@link Session} is representative of the session that submitted the operation
   * that resulted in this {@link RaftCommit}. The session can be used to {@link RaftSession#publish(String, Object)}
   * event messages to the client.
   *
   * @return The session that created the commit.
   */
  RaftSession session();

  /**
   * Returns the time at which the operation was committed.
   * <p>
   * The time is representative of the time at which the leader wrote the operation to its log. Because instants
   * are replicated through the Raft consensus algorithm, they are guaranteed to be consistent across all servers
   * and therefore can be used to perform time-dependent operations such as expiring keys or timeouts. Additionally,
   * commit times are guaranteed to progress monotonically, never going back in time.
   * <p>
   * Users should <em>never</em> use {@code System} time to control behavior in a state machine and should instead rely
   * upon {@link RaftCommit} times or use the {@link StateMachineExecutor} for time-based controls.
   *
   * @return The commit time.
   */
  Instant time();

  /**
   * Returns the commit type.
   * <p>
   * This is the {@link Class} returned by the committed operation's {@link Object#getClass()} method.
   *
   * @return The commit type.
   */
  Class<T> type();

  /**
   * Returns the operation submitted by the client.
   *
   * @return The operation submitted by the client.
   */
  T operation();

  /**
   * Returns the command submitted by the client.
   * <p>
   * This method is an alias for the {@link #operation()} method. It is intended to aid with clarity in code.
   * This method does <em>not</em> perform any type checking of the operation to ensure it is in fact a
   * {@link Command} object.
   *
   * @return The command submitted by the client.
   */
  default T command() {
    return operation();
  }

  /**
   * Returns the query submitted by the client.
   * <p>
   * This method is an alias for the {@link #operation()} method. It is intended to aid with clarity in code.
   * This method does <em>not</em> perform any type checking of the operation to ensure it is in fact a
   * {@link Query} object.
   *
   * @return The query submitted by the client.
   */
  default T query() {
    return operation();
  }

}
