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

import net.kuujo.copycat.raft.server.storage.OperationEntry;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Server commit pool.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerCommitPool implements AutoCloseable {
  private final ServerCommitCleaner cleaner;
  private final SessionManager sessions;
  private final Queue<ServerCommit> pool = new ConcurrentLinkedQueue<>();

  public ServerCommitPool(ServerCommitCleaner cleaner, SessionManager sessions) {
    this.cleaner = cleaner;
    this.sessions = sessions;
  }

  /**
   * Acquires a commit from the pool.
   *
   * @param entry The entry for which to acquire the commit.
   * @return The commit to acquire.
   */
  public ServerCommit acquire(OperationEntry entry) {
    ServerCommit commit = pool.poll();
    if (commit == null) {
      commit = new ServerCommit(this, cleaner, sessions);
    }
    commit.reset(entry);
    return commit;
  }

  /**
   * Releases a commit back to the pool.
   *
   * @param commit The commit to release.
   */
  public void release(ServerCommit commit) {
    pool.add(commit);
  }

  @Override
  public void close() {
    pool.clear();
  }

}
