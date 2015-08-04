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
package net.kuujo.copycat.collections.state;

import net.kuujo.copycat.PersistenceLevel;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.server.Apply;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.StateMachine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Distributed set state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SetState extends StateMachine {
  private final Map<Integer, Commit<? extends SetCommands.TtlCommand>> map = new HashMap<>();
  private final Set<Long> sessions = new HashSet<>();
  private long time;

  /**
   * Updates the wall clock time.
   */
  private void updateTime(Commit<?> commit) {
    time = Math.max(time, commit.timestamp());
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
  private boolean isActive(Commit<? extends SetCommands.TtlCommand> commit) {
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
   * Handles a contains commit.
   */
  @Apply(SetCommands.Contains.class)
  protected boolean contains(Commit<SetCommands.Contains> commit) {
    updateTime(commit);
    Commit<? extends SetCommands.TtlCommand> command = map.get(commit.operation().value());
    if (!isActive(command)) {
      map.remove(commit.operation().value());
      return false;
    }
    return true;
  }

  /**
   * Handles an add commit.
   */
  @Apply(SetCommands.Add.class)
  protected boolean put(Commit<SetCommands.Add> commit) {
    updateTime(commit);
    Commit<? extends SetCommands.TtlCommand> command = map.get(commit.operation().value());
    if (!isActive(command)) {
      map.put(commit.operation().value(), commit);
      return true;
    }
    return false;
  }

  /**
   * Handles a remove commit.
   */
  @Apply(SetCommands.Remove.class)
  protected boolean remove(Commit<SetCommands.Remove> commit) {
    updateTime(commit);
    Commit<? extends SetCommands.TtlCommand> command = map.remove(commit.operation().value());
    return isActive(command);
  }

  /**
   * Handles a count commit.
   */
  @Apply(SetCommands.Size.class)
  protected int size(Commit<SetCommands.Size> commit) {
    updateTime(commit);
    return map.size();
  }

  /**
   * Handles an is empty commit.
   */
  @Apply(SetCommands.IsEmpty.class)
  protected boolean isEmpty(Commit<SetCommands.IsEmpty> commit) {
    updateTime(commit);
    return map.isEmpty();
  }

  /**
   * Handles a clear commit.
   */
  @Apply(SetCommands.Clear.class)
  protected void clear(Commit<SetCommands.Clear> commit) {
    updateTime(commit);
    map.clear();
  }

}
