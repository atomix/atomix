/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Server state machine context.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
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

  private final long id;
  private final String name;
  private final String type;
  private final InternalClock clock = new InternalClock();
  private final RaftServerStateMachineSessions sessions;
  private Type context;
  private long index;

  RaftServerStateMachineContext(long id, String name, String type, RaftServerStateMachineSessions sessions) {
    this.id = id;
    this.name = name;
    this.type = type;
    this.sessions = sessions;
  }

  /**
   * Updates the state machine context.
   */
  public void update(long index, Instant instant, Type context) {
    this.index = index;
    this.context = context;
    clock.set(instant);
  }

  /**
   * Commits the state machine index.
   */
  public void commit() {
    long index = this.index;
    for (RaftSessionContext session : sessions.sessions.values()) {
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
  public long id() {
    return id;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String type() {
    return type;
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public Clock clock() {
    return clock;
  }

  @Override
  public RaftServerStateMachineSessions sessions() {
    return sessions;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index)
        .add("time", clock)
        .toString();
  }

  /**
   * Internal clock implementation.
   */
  private static final class InternalClock extends Clock {
    private final ZoneId zoneId = ZoneId.of("UTC");
    private Instant instant;

    /**
     * Sets the state machine time instant.
     */
    void set(Instant instant) {
      this.instant = instant;
    }

    @Override
    public ZoneId getZone() {
      return zoneId;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      throw new UnsupportedOperationException("cannot modify state machine time zone");
    }

    @Override
    public Instant instant() {
      return instant;
    }
  }
}