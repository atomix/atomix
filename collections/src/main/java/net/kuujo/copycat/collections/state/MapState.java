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
 * Map state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MapState extends StateMachine {
  private Map<Object, Commit<? extends MapCommands.TtlCommand>> map;
  private final Set<Long> sessions = new HashSet<>();

  @Override
  public void configure(StateMachineExecutor executor) {
    executor.register(MapCommands.ContainsKey.class, this::containsKey);
    executor.register(MapCommands.Get.class, this::get);
    executor.register(MapCommands.GetOrDefault.class, this::getOrDefault);
    executor.register(MapCommands.Put.class, this::put);
    executor.register(MapCommands.PutIfAbsent.class, this::putIfAbsent);
    executor.register(MapCommands.Remove.class, this::remove);
    executor.register(MapCommands.Size.class, this::size);
    executor.register(MapCommands.IsEmpty.class, this::isEmpty);
    executor.register(MapCommands.Clear.class, this::clear);
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
  private boolean isActive(Commit<? extends MapCommands.TtlCommand> commit, Instant instant) {
    if (commit == null) {
      return false;
    } else if (commit.operation().mode() == PersistenceLevel.EPHEMERAL && !sessions.contains(commit.session().id())) {
      return false;
    } else if (commit.operation().ttl() != 0 && commit.operation().ttl() < instant.toEpochMilli() - commit.time().toEpochMilli()) {
      return false;
    }
    return true;
  }

  /**
   * Handles a contains key commit.
   */
  protected boolean containsKey(Commit<MapCommands.ContainsKey> commit) {
    try {
      if (map == null) {
        return false;
      }

      Commit<? extends MapCommands.TtlCommand> command = map.get(commit.operation().key());
      if (!isActive(command, context().time().instant())) {
        map.remove(commit.operation().key());
        return false;
      }
      return true;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get commit.
   */
  protected Object get(Commit<MapCommands.Get> commit) {
    if (map == null) {
      return null;
    }

    try {
      Commit<? extends MapCommands.TtlCommand> command = map.get(commit.operation().key());
      if (command != null) {
        if (!isActive(command, context().time().instant())) {
          map.remove(commit.operation().key());
        } else {
          return command.operation().value();
        }
      }
      return null;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get or default commit.
   */
  protected Object getOrDefault(Commit<MapCommands.GetOrDefault> commit) {
    if (map == null) {
      return commit.operation().defaultValue();
    }

    try {
      Commit<? extends MapCommands.TtlCommand> previous = map.get(commit.operation().key());
      if (previous == null) {
        return commit.operation().defaultValue();
      } else if (isActive(previous, context().time().instant())) {
        return previous.operation().value();
      }
      return commit.operation().defaultValue();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a put commit.
   */
  protected Object put(Commit<MapCommands.Put> commit) {
    if (map == null) {
      map = new HashMap<>();
    }

    Commit<? extends MapCommands.TtlCommand> previous = map.get(commit.operation().key());
    if (previous == null) {
      if (!isActive(commit, context().time().instant())) {
        commit.clean();
      } else {
        map.put(commit.operation().key(), commit);
      }
      return null;
    } else {
      map.put(commit.operation().key(), commit);
      previous.clean();
      return isActive(previous, commit.time()) ? previous.operation().value() : null;
    }
  }

  /**
   * Handles a put if absent commit.
   */
  protected Object putIfAbsent(Commit<MapCommands.PutIfAbsent> commit) {
    if (map == null) {
      map = new HashMap<>();
    }

    Commit<? extends MapCommands.TtlCommand> previous = map.get(commit.operation().key());
    if (previous == null) {
      if (!isActive(commit, context().time().instant())) {
        commit.clean();
      } else {
        map.put(commit.operation().key(), commit);
      }
      return null;
    } else {
      if (!isActive(previous, commit.time())) {
        map.put(commit.operation().key(), commit);
        previous.clean();
        return null;
      } else {
        return previous.operation().value();
      }
    }
  }

  /**
   * Handles a remove commit.
   */
  protected Object remove(Commit<MapCommands.Remove> commit) {
    if (map == null) {
      commit.clean();
      return null;
    } else if (commit.operation().value() != null) {
      Commit<? extends MapCommands.TtlCommand> previous = map.get(commit.operation().key());
      if (previous == null) {
        commit.clean();
        return true;
      } else if (!isActive(previous, commit.time())) {
        map.remove(commit.operation().key());
        previous.clean();
      } else {
        Object value = previous.operation().value();
        if ((value == null && commit.operation().value() == null) || (value != null && commit.operation().value() != null && value.equals(commit.operation().value()))) {
          map.remove(commit.operation().key());
          previous.clean();
          return true;
        }
        return false;
      }
      return false;
    } else {
      Commit<? extends MapCommands.TtlCommand> previous = map.remove(commit.operation().key());
      if (previous == null) {
        commit.clean();
        return true;
      } else {
        previous.clean();
        return isActive(previous, commit.time()) ? previous.operation().value() : null;
      }
    }
  }

  /**
   * Handles a count commit.
   */
  protected int size(Commit<MapCommands.Size> commit) {
    try {
      return map != null ? map.size() : 0;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an is empty commit.
   */
  protected boolean isEmpty(Commit<MapCommands.IsEmpty> commit) {
    try {
      return map == null || map.isEmpty();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a clear commit.
   */
  protected void clear(Commit<MapCommands.Clear> commit) {
    if (map == null) {
      commit.clean();
    } else {
      map.clear();
    }
  }

}
