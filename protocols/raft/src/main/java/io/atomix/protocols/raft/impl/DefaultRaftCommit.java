/*
 * Copyright 2015-present Open Networking Laboratory
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
package io.atomix.protocols.raft.impl;

import io.atomix.protocols.raft.RaftCommit;
import io.atomix.protocols.raft.RaftOperation;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.time.LogicalTimestamp;
import io.atomix.time.WallClockTimestamp;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Server commit.
 */
public class DefaultRaftCommit implements RaftCommit<RaftOperation<?>> {
  private final long index;
  private final RaftSession session;
  private final long timestamp;
  private final RaftOperation operation;

  public DefaultRaftCommit(long index, RaftOperation operation, RaftSession session, long timestamp) {
    this.index = index;
    this.session = session;
    this.timestamp = timestamp;
    this.operation = operation;
  }

  @Override
  public long getIndex() {
    return index;
  }

  @Override
  public RaftSession getSession() {
    return session;
  }

  @Override
  public LogicalTimestamp getLogicalTime() {
    return LogicalTimestamp.of(index);
  }

  @Override
  public WallClockTimestamp getWallClockTime() {
    return WallClockTimestamp.of(timestamp);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class getType() {
    return operation != null ? operation.getClass() : null;
  }

  @Override
  public RaftOperation<?> getOperation() {
    return operation;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index)
        .add("session", session)
        .add("time", getWallClockTime())
        .add("operation", operation)
        .toString();
  }

}
