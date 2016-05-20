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
package io.atomix.manager.internal;

import io.atomix.copycat.Operation;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.manager.resource.internal.InstanceOperation;

import java.time.Instant;

/**
 * Resource commit.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ResourceManagerCommit implements Commit {
  private final ResourceManagerCommitPool pool;
  private Commit<InstanceOperation<?, ?>> commit;
  private ServerSession session;

  public ResourceManagerCommit(ResourceManagerCommitPool pool) {
    this.pool = pool;
  }

  /**
   * Resets the resource commit.
   *
   * @param commit The parent commit.
   * @param session The resource session.
   */
  void reset(Commit<InstanceOperation<?, ?>> commit, ServerSession session) {
    this.commit = commit;
    this.session = session;
  }

  @Override
  public long index() {
    return commit.index();
  }

  @Override
  public ServerSession session() {
    return session;
  }

  @Override
  public Instant time() {
    return commit.time();
  }

  @Override
  public Class<?> type() {
    return commit.operation().operation().getClass();
  }

  @Override
  public Operation<?> operation() {
    return commit.operation().operation();
  }

  @Override
  public Commit acquire() {
    commit.acquire();
    return this;
  }

  @Override
  public boolean release() {
    if (commit.release()) {
      pool.release(this);
      return true;
    }
    return false;
  }

  @Override
  public int references() {
    return commit.references();
  }

  @Override
  public void close() {
    commit.close();
    pool.release(this);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, session=%s, time=%s, operation=%s]", getClass().getSimpleName(), index(), session(), time(), operation());
  }

}
