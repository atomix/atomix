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
package net.kuujo.copycat.raft.server.state;

import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.storage.OperationEntry;

/**
 * Server commit.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerCommit implements Commit {
  private final ServerCommitPool pool;
  private final ServerCommitCleaner cleaner;
  private final SessionManager sessions;
  private OperationEntry entry;
  private Session session;

  public ServerCommit(ServerCommitPool pool, ServerCommitCleaner cleaner, SessionManager sessions) {
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
    this.entry = entry;
    this.session = sessions.getSession(entry.getSession());
  }

  @Override
  public long index() {
    return entry.getIndex();
  }

  @Override
  public Session session() {
    return session;
  }

  @Override
  public long timestamp() {
    return entry.getTimestamp();
  }

  @Override
  public Class type() {
    return entry.getOperation().getClass();
  }

  @Override
  public Operation operation() {
    return entry.getOperation();
  }

  @Override
  public void clean() {
    cleaner.clean(entry);
    pool.release(this);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, session=%s, timestamp=%d, operation=%s]", getClass().getSimpleName(), index(), session(), timestamp(), operation());
  }

}
