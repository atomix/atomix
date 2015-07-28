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
package net.kuujo.copycat.atomic.state;

import net.kuujo.copycat.PersistenceLevel;
import net.kuujo.copycat.raft.log.Compaction;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.server.Apply;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.Filter;
import net.kuujo.copycat.raft.server.StateMachine;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Atomic reference state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReferenceState extends StateMachine {
  private final java.util.Set<Long> sessions = new HashSet<>();
  private final java.util.Set<Session> listeners = new HashSet<>();
  private final AtomicReference<Object> value = new AtomicReference<>();
  private Commit<? extends ReferenceCommands.ReferenceCommand> command;
  private long version;
  private long time;

  /**
   * Updates the state machine timestamp.
   */
  private void updateTime(Commit commit) {
    this.time = Math.max(time, commit.timestamp());
  }

  @Override
  public void register(Session session) {
    sessions.add(session.id());
  }

  @Override
  public void expire(Session session) {
    sessions.remove(session.id());
  }

  @Override
  public void close(Session session) {
    sessions.remove(session.id());
  }

  /**
   * Returns a boolean value indicating whether the given commit is active.
   */
  private boolean isActive(Commit<? extends ReferenceCommands.ReferenceCommand> commit) {
    if (commit == null) {
      return false;
    } else if (commit.operation().mode() == PersistenceLevel.EPHEMERAL && !sessions.contains(commit.session().id())) {
      return false;
    } else if (commit.operation().ttl() != 0 && commit.operation().ttl() < time - commit.timestamp()) {
      return false;
    }
    return true;
  }

  /**
   * Handles a listen commit.
   */
  @Apply(ReferenceCommands.Listen.class)
  protected void listen(Commit<ReferenceCommands.Listen> commit) {
    updateTime(commit);
    listeners.add(commit.session());
  }

  /**
   * Handles an unlisten commit.
   */
  @Apply(ReferenceCommands.Unlisten.class)
  protected void unlisten(Commit<ReferenceCommands.Unlisten> commit) {
    updateTime(commit);
    listeners.remove(commit.session());
  }

  /**
   * Triggers a change event.
   */
  private void change(Object value) {
    for (Session session : listeners) {
      session.publish(value);
    }
  }

  /**
   * Handles a get commit.
   */
  @Apply(ReferenceCommands.Get.class)
  protected Object get(Commit<ReferenceCommands.Get> commit) {
    updateTime(commit);
    return value.get();
  }

  /**
   * Applies a set commit.
   */
  @Apply(ReferenceCommands.Set.class)
  protected void set(Commit<ReferenceCommands.Set> commit) {
    updateTime(commit);
    value.set(commit.operation().value());
    command = commit;
    version = commit.index();
    change(value.get());
  }

  /**
   * Handles a compare and set commit.
   */
  @Apply(ReferenceCommands.CompareAndSet.class)
  protected boolean compareAndSet(Commit<ReferenceCommands.CompareAndSet> commit) {
    updateTime(commit);
    if (isActive(command)) {
      if (value.compareAndSet(commit.operation().expect(), commit.operation().update())) {
        command = commit;
        change(value.get());
        return true;
      }
      return false;
    } else if (commit.operation().expect() == null) {
      value.set(null);
      command = commit;
      version = commit.index();
      change(null);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Handles a get and set commit.
   */
  @Apply(ReferenceCommands.GetAndSet.class)
  protected Object getAndSet(Commit<ReferenceCommands.GetAndSet> commit) {
    updateTime(commit);
    if (isActive(command)) {
      Object result = value.getAndSet(commit.operation().value());
      command = commit;
      version = commit.index();
      change(value.get());
      return result;
    } else {
      value.set(commit.operation().value());
      command = commit;
      version = commit.index();
      change(value.get());
      return null;
    }
  }

  /**
   * Filters all entries.
   */
  @Filter(Filter.All.class)
  protected boolean filterAll(Commit<? extends ReferenceCommands.ReferenceCommand<?>> commit, Compaction compaction) {
    return commit.index() >= version && isActive(commit);
  }

}
