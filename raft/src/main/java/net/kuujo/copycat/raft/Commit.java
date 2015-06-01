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
package net.kuujo.copycat.raft;

import net.kuujo.copycat.cluster.Session;

/**
 * Protocol commit.
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
   *
   * @return The commit index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the commit session.
   *
   * @return The commit session.
   */
  public Session session() {
    return session;
  }

  /**
   * Returns the commit timestamp.
   *
   * @return the commit timestamp.
   */
  public long timestamp() {
    return timestamp;
  }

  /**
   * Returns the commit type.
   *
   * @return The commit type.
   */
  @SuppressWarnings("unchecked")
  public Class<T> type() {
    return (Class<T>) operation.getClass();
  }

  /**
   * Returns the operation.
   *
   * @return The operation.
   */
  public T operation() {
    return operation;
  }

  @Override
  public String toString() {
    return String.format("Commit[index=%d, timestamp=%d, session=%s, operation=%s]", index, timestamp, session, operation);
  }

}
