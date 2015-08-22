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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.raft.protocol.Operation;
import net.kuujo.copycat.raft.session.Session;
import net.kuujo.copycat.raft.Commit;
import net.kuujo.copycat.raft.storage.OperationEntry;

import java.time.Instant;

/**
 * Server commit.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerCommit implements Commit {
  private final ServerCommitPool pool;
  private final ServerCommitCleaner cleaner;
  private final ServerSessionManager sessions;
  private long index;
  private Session session;
  private Instant instant;
  private Operation operation;
  private volatile boolean open;

  public ServerCommit(ServerCommitPool pool, ServerCommitCleaner cleaner, ServerSessionManager sessions) {
    this.pool = pool;
    this.cleaner = cleaner;
    this.sessions = sessions;
  }

  /**
   * Resets the commit.
   *
   * @param entry The entry.
   */
  void reset(OperationEntry entry) {
    this.index = entry.getIndex();
    this.session = sessions.getSession(entry.getSession());
    this.instant = Instant.ofEpochMilli(entry.getTimestamp());
    this.operation = entry.getOperation();
    open = true;
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public Session session() {
    return session;
  }

  @Override
  public Instant time() {
    return instant;
  }

  @Override
  public Class type() {
    return operation.getClass();
  }

  @Override
  public Operation operation() {
    return operation;
  }

  @Override
  public void clean() {
    if (!open)
      throw new IllegalStateException("commit closed");
    cleaner.clean(index);
    close();
  }

  @Override
  public void close() {
    if (open) {
      pool.release(this);
      open = false;
    }
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, session=%s, time=%s, operation=%s]", getClass().getSimpleName(), index(), session(), time(), operation());
  }

}
