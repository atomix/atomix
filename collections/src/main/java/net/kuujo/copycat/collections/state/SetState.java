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

import net.kuujo.copycat.PersistenceMode;
import net.kuujo.copycat.raft.session.Session;
import net.kuujo.copycat.raft.Commit;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.StateMachineExecutor;

import java.time.Instant;
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
  private Map<Integer, Commit<? extends SetCommands.TtlCommand>> map;
  private final Set<Long> sessions = new HashSet<>();

  @Override
  public void configure(StateMachineExecutor executor) {
    executor.register(SetCommands.Add.class, this::add);
    executor.register(SetCommands.Contains.class, this::contains);
    executor.register(SetCommands.Remove.class, this::remove);
    executor.register(SetCommands.Size.class, this::size);
    executor.register(SetCommands.IsEmpty.class, this::isEmpty);
    executor.register(SetCommands.Clear.class, this::clear);
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
  private boolean isActive(Commit<? extends SetCommands.TtlCommand> commit, Instant instant) {
    if (commit == null) {
      return false;
    } else if (commit.operation().mode() == PersistenceMode.EPHEMERAL && !sessions.contains(commit.session().id())) {
      return false;
    } else if (commit.operation().ttl() != 0 && commit.operation().ttl() < instant.toEpochMilli() - commit.time().toEpochMilli()) {
      return false;
    }
    return true;
  }

  /**
   * Handles a contains commit.
   */
  protected boolean contains(Commit<SetCommands.Contains> commit) {
    try {
      if (map == null) {
        return false;
      }

      Commit<? extends SetCommands.TtlCommand> command = map.get(commit.operation().value());
      if (!isActive(command, commit.time())) {
        map.remove(commit.operation().value());
        return false;
      }
      return true;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an add commit.
   */
  protected boolean add(Commit<SetCommands.Add> commit) {
    if (map == null) {
      map = new HashMap<>();
    }

    Commit<? extends SetCommands.TtlCommand> previous = map.get(commit.operation().value());
    if (!isActive(commit, context().time().instant())) {
      commit.clean();
      return false;
    } else if (!isActive(previous, commit.time())) {
      if (previous != null)
        previous.clean();
      map.put(commit.operation().value(), commit);
      return true;
    } else {
      commit.clean();
      return false;
    }
  }

  /**
   * Handles a remove commit.
   */
  protected boolean remove(Commit<SetCommands.Remove> commit) {
    if (map == null) {
      commit.clean();
      return false;
    } else {
      Commit<? extends SetCommands.TtlCommand> previous = map.remove(commit.operation().value());
      if (previous == null) {
        commit.clean();
        return false;
      } else {
        previous.clean();
        return isActive(previous, commit.time());
      }
    }
  }

  /**
   * Handles a count commit.
   */
  protected int size(Commit<SetCommands.Size> commit) {
    try {
      return map != null ? map.size() : 0;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an is empty commit.
   */
  protected boolean isEmpty(Commit<SetCommands.IsEmpty> commit) {
    try {
      return map == null || map.isEmpty();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a clear commit.
   */
  protected void clear(Commit<SetCommands.Clear> commit) {
    if (map == null) {
      commit.clean();
    } else {
      map.clear();
    }
  }

}
