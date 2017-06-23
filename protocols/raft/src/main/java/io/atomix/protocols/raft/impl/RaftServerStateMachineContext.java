/*
 * Copyright 2017-present Open Networking Laboratory
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

import io.atomix.protocols.raft.StateMachineContext;
import io.atomix.protocols.raft.session.impl.RaftSessionContext;
import io.atomix.protocols.raft.storage.snapshot.StateMachineId;
import io.atomix.time.LogicalClock;
import io.atomix.time.LogicalTimestamp;
import io.atomix.time.WallClock;
import io.atomix.time.WallClockTimestamp;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Server state machine context.
 */
public class RaftServerStateMachineContext implements StateMachineContext {

  /**
   * Context type.
   */
  public enum Type {
    COMMAND,
    QUERY,
    SNAPSHOT,
  }

  private final StateMachineId id;
  private final String name;
  private final String type;
  private final RaftServerStateMachineSessions sessions;
  private final LogicalClock logicalClock = new LogicalClock() {
    @Override
    public LogicalTimestamp getTime() {
      return new LogicalTimestamp(index);
    }
  };
  private final WallClock wallClock = new WallClock() {
    @Override
    public WallClockTimestamp getTime() {
      return new WallClockTimestamp(timestamp);
    }
  };
  private long timestamp;
  private Type context;
  private long index;

  RaftServerStateMachineContext(StateMachineId id, String name, String type, RaftServerStateMachineSessions sessions) {
    this.id = id;
    this.name = name;
    this.type = type;
    this.sessions = sessions;
  }

  /**
   * Updates the state machine context.
   */
  public void update(long index, long timestamp, Type context) {
    this.index = index;
    this.timestamp = timestamp;
    this.context = context;
  }

  /**
   * Commits the state machine index.
   */
  public void commit() {
    long index = this.index;
    for (RaftSessionContext session : sessions.getSessions()) {
      session.commit(index);
    }
  }

  /**
   * Returns the current context type.
   */
  public Type context() {
    return context;
  }

  @Override
  public StateMachineId stateMachineId() {
    return id;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String typeName() {
    return type;
  }

  @Override
  public long currentIndex() {
    return index;
  }

  @Override
  public LogicalClock logicalClock() {
    return logicalClock;
  }

  @Override
  public WallClock wallClock() {
    return wallClock;
  }

  @Override
  public RaftServerStateMachineSessions sessions() {
    return sessions;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index)
        .add("time", wallClock)
        .toString();
  }
}