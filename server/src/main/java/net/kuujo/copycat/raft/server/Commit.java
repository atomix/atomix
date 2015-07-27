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

/**
 * Represents the committed state and metadata of a Raft state machine operation.
 * <p>
 * This class is used by the {@link net.kuujo.copycat.raft.server.RaftServer} internally to expose committed
 * {@link net.kuujo.copycat.raft.Command commands} and {@link net.kuujo.copycat.raft.Query queries} to the user provided
 * {@link net.kuujo.copycat.raft.server.StateMachine}. Operations that are wrapped in a {@link net.kuujo.copycat.raft.server.Commit}
 * object are guaranteed to be persisted in the Raft log on a majority of the cluster. The commit object provides metadata
 * about the committed operation such as the {@link #index()} at which the operation was written in the log, the
 * {@link #timestamp()} at which it was written, and the {@link net.kuujo.copycat.raft.Session} that submitted the
 * operation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Commit<T extends Operation> {
  private final long index;
  private final long timestamp;
  private final Session session;
  private final T operation;

  public Commit(long index, Session session, long timestamp, T operation) {
    this.index = index;
    this.session = session;
    this.timestamp = timestamp;
    this.operation = operation;
  }

  /**
   * Returns the commit index.
   * <p>
   * This is the index at which the committed {@link net.kuujo.copycat.raft.Operation} was written in the Raft log.
   * Copycat guarantees that this index will be the same for all instances of the given operation on all nodes in the
   * cluster.
   * <p>
   * Note that for {@link net.kuujo.copycat.raft.Query} operations, the returned {@code index} may actually represent
   * the last committed index in the Raft log since queries are not actually written to disk. This, however, does not
   * break the single index guarantee since queries will only be applied to the leader's {@link net.kuujo.copycat.raft.server.StateMachine}.
   *
   * @return The commit index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the session that submitted the operation.
   * <p>
   * The returned {@link net.kuujo.copycat.raft.Session} is representative of the session that submitted the operation
   * that resulted in this {@link net.kuujo.copycat.raft.server.Commit}. Note that the session will be provided regardless
   * of whether the session is currently active in the cluster. However, attempts to {@link Session#publish(Object)} to
   * inactive sessions will fail silently.
   * <p>
   * Additionally, because clients only connect to one server at any given time, the {@link net.kuujo.copycat.raft.Session}
   * provided in any given commit to any {@link net.kuujo.copycat.raft.server.StateMachine} may or may not be capable of
   * communicating with the client.
   *
   * @return The session that created the commit.
   */
  public Session session() {
    return session;
  }

  /**
   * Returns the commit timestamp.
   * <p>
   * The timestamp is representative of the time at which the leader wrote the operation to its log. Because commit
   * timestamps are replicated through the Raft consensus algorithm, they are guaranteed to be consistent across all
   * servers and therefore can be used to perform time-dependent operations such as expiring keys or timeouts.
   * <p>
   * Users should <em>never</em> use {@code System} time to control behavior in a state machine and should instead rely
   * upon {@link net.kuujo.copycat.raft.server.Commit} timestamps for time-based controls.
   *
   * @return the commit timestamp.
   */
  public long timestamp() {
    return timestamp;
  }

  /**
   * Returns the commit type.
   * <p>
   * This is the {@link java.lang.Class} returned by the committed operation's {@link Object#getClass()} method.
   *
   * @return The commit type.
   */
  @SuppressWarnings("unchecked")
  public Class<T> type() {
    return (Class<T>) operation.getClass();
  }

  /**
   * Returns the operation submitted by the user.
   * <p>
   * The returned {@link net.kuujo.copycat.raft.Operation} is the operation submitted by the user via
   * {@link net.kuujo.copycat.raft.Raft#submit(net.kuujo.copycat.raft.Operation)}.
   *
   * @return The operation submitted by the user.
   */
  public T operation() {
    return operation;
  }

  @Override
  public String toString() {
    return String.format("Commit[index=%d, timestamp=%d, session=%s, operation=%s]", index, timestamp, session, operation);
  }

}
