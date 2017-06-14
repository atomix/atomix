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
package io.atomix.protocols.raft.server.state;

import io.atomix.protocols.raft.RaftOperation;
import io.atomix.protocols.raft.server.RaftCommit;
import io.atomix.protocols.raft.server.session.ServerSession;
import io.atomix.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Server commit.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class ServerCommit implements RaftCommit<RaftOperation<?>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerCommit.class);
  private final AtomicInteger references = new AtomicInteger(1);
  private final long index;
  private final ServerSessionContext session;
  private final Instant instant;
  private final RaftOperation operation;

  public ServerCommit(long index, RaftOperation operation, ServerSessionContext session, long timestamp) {
    this.index = index;
    this.session = session;
    this.instant = Instant.ofEpochMilli(timestamp);
    this.operation = operation;
  }

  /**
   * Checks whether the commit is open and throws an exception if not.
   */
  private void checkOpen() {
    Assert.state(references.get() > 0, "commit not open");
  }

  @Override
  public long index() {
    checkOpen();
    return index;
  }

  @Override
  public ServerSession session() {
    checkOpen();
    return session;
  }

  @Override
  public Instant time() {
    checkOpen();
    return instant;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class type() {
    checkOpen();
    return operation != null ? operation.getClass() : null;
  }

  @Override
  public RaftOperation<?> operation() {
    checkOpen();
    return operation;
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, session=%s, time=%s, operation=%s]", getClass().getSimpleName(), index(), session(), time(), operation());
  }

}
