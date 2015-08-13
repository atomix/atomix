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
package net.kuujo.copycat.raft.server;

import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.Session;

import java.time.Instant;

/**
 * Represents the committed state and metadata of a Raft state machine operation.
 * <p>
 * This class is used by the {@link RaftServer} internally to expose committed
 * {@link net.kuujo.copycat.raft.Command commands} and {@link net.kuujo.copycat.raft.Query queries} to the user provided
 * {@link StateMachine}. Operations that are wrapped in a {@link Commit}
 * object are guaranteed to be persisted in the Raft log on a majority of the cluster. The commit object provides metadata
 * about the committed operation such as the {@link #index()} at which the operation was written in the log, the
 * {@link #time()} at which it was written, and the {@link net.kuujo.copycat.raft.Session} that submitted the
 * operation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Commit<T extends Operation> extends AutoCloseable {

  /**
   * Returns the commit index.
   * <p>
   * This is the index at which the committed {@link net.kuujo.copycat.raft.Operation} was written in the Raft log.
   * Copycat guarantees that this index will be the same for all instances of the given operation on all nodes in the
   * cluster.
   * <p>
   * Note that for {@link net.kuujo.copycat.raft.Query} operations, the returned {@code index} may actually represent
   * the last committed index in the Raft log since queries are not actually written to disk. This, however, does not
   * break the single index guarantee since queries will only be applied to the leader's {@link StateMachine}.
   *
   * @return The commit index.
   */
  long index();

  /**
   * Returns the session that submitted the operation.
   * <p>
   * The returned {@link net.kuujo.copycat.raft.Session} is representative of the session that submitted the operation
   * that resulted in this {@link Commit}. Note that the session will be provided regardless
   * of whether the session is currently active in the cluster. However, attempts to {@link Session#publish(Object)} to
   * inactive sessions will fail silently.
   * <p>
   * Additionally, because clients only connect to one server at any given time, the {@link net.kuujo.copycat.raft.Session}
   * provided in any given commit to any {@link StateMachine} may or may not be capable of
   * communicating with the client.
   *
   * @return The session that created the commit.
   */
  Session session();

  /**
   * Returns the time at which the operation was committed.
   * <p>
   * The time is representative of the time at which the leader wrote the operation to its log. Because instants
   * are replicated through the Raft consensus algorithm, they are guaranteed to be consistent across all servers
   * and therefore can be used to perform time-dependent operations such as expiring keys or timeouts.
   * <p>
   * Users should <em>never</em> use {@code System} time to control behavior in a state machine and should instead rely
   * upon {@link Commit} times for time-based controls.
   *
   * @return The commit time.
   */
  Instant time();

  /**
   * Returns the commit type.
   * <p>
   * This is the {@link java.lang.Class} returned by the committed operation's {@link Object#getClass()} method.
   *
   * @return The commit type.
   */
  Class<T> type();

  /**
   * Returns the operation submitted by the user.
   *
   * @return The operation submitted by the user.
   */
  T operation();

  /**
   * Cleans the commit.
   * <p>
   * When the commit is cleaned, it will be removed from the log and may be removed permanently from disk at some
   * arbitrary point in the future.
   */
  void clean();

  /**
   * Closes the commit.
   * <p>
   * Once the commit is closed, it may be recycled and should no longer be accessed by the closer.
   */
  @Override
  void close();

}
