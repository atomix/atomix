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
package net.kuujo.copycat.manager;

import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.resource.ResourceOperation;

/**
 * Resource commit.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ResourceCommit implements Commit {
  private final ResourceCommitPool pool;
  private Commit<ResourceOperation> commit;
  private Session session;

  public ResourceCommit(ResourceCommitPool pool) {
    this.pool = pool;
  }

  /**
   * Resets the resource commit.
   *
   * @param commit The parent commit.
   * @param session The resource session.
   */
  void reset(Commit<ResourceOperation> commit, Session session) {
    this.commit = commit;
    this.session = session;
  }

  @Override
  public long index() {
    return commit.index();
  }

  @Override
  public Session session() {
    return session;
  }

  @Override
  public long timestamp() {
    return commit.timestamp();
  }

  @Override
  public Class type() {
    return commit.operation().operation().getClass();
  }

  @Override
  public Operation operation() {
    return commit.operation().operation();
  }

  @Override
  public void clean() {
    commit.clean();
    close();
  }

  @Override
  public void close() {
    pool.release(this);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, session=%s, timestamp=%d, operation=%s]", getClass().getSimpleName(), index(), session(), timestamp(), operation());
  }

}
