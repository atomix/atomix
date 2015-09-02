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
import net.kuujo.copycat.raft.Commit;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.StateMachineExecutor;
import net.kuujo.copycat.raft.session.Session;
import net.kuujo.copycat.util.Listener;
import net.kuujo.copycat.util.concurrent.Scheduled;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Map state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MapState extends StateMachine {
  private StateMachineExecutor executor;
  private Map<Object, Commit<? extends MapCommands.TtlCommand>> map;
  private final Map<Object, Scheduled> timers = new HashMap<>();
  private final Map<Object, Listener<Session>> listeners = new HashMap<>();

  @Override
  public void configure(StateMachineExecutor executor) {
    this.executor = executor;
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

  /**
   * Returns a boolean value indicating whether the given commit is active.
   */
  private boolean isActive(Commit<? extends MapCommands.TtlCommand> commit, Instant instant) {
    if (commit == null) {
      return false;
    } else if (commit.operation().mode() == PersistenceMode.EPHEMERAL && sessions().session(commit.session().id()) == null) {
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
      return isActive(command, commit.time());
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
      return isActive(command, commit.time()) ? command.operation().value() : null;
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
      return isActive(previous, commit.time()) ? previous.operation().value() : commit.operation().defaultValue();
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
      if (commit.operation().ttl() > 0) {
        timers.put(commit.operation().key(), executor.schedule(() -> {
          map.remove(commit.operation().key());
          Listener<Session> listener = listeners.remove(commit.operation().key());
          if (listener != null)
            listener.close();
          commit.clean();
        }, Duration.ofMillis(commit.operation().ttl())));
      }

      if (commit.operation().mode() == PersistenceMode.EPHEMERAL) {
        listeners.put(commit.operation().key(), commit.session().onClose(s -> {
          map.remove(commit.operation().key());
          Scheduled scheduled = timers.remove(commit.operation().key());
          if (scheduled != null)
            scheduled.cancel();
          commit.clean();
        }));
      }
    } else {
      previous.clean();

      if (commit.operation().ttl() > 0) {
        timers.put(commit.operation().key(), executor.schedule(() -> {
          map.remove(commit.operation().key());
          Listener<Session> listener = listeners.remove(commit.operation().key());
          if (listener != null)
            listener.close();
          commit.close();
        }, Duration.ofMillis(commit.operation().ttl())));
      }

      if (commit.operation().mode() == PersistenceMode.EPHEMERAL) {
        listeners.put(commit.operation().key(), commit.session().onClose(s -> {
          map.remove(commit.operation().key());
          Scheduled scheduled = timers.remove(commit.operation().key());
          if (scheduled != null)
            scheduled.cancel();
          commit.close();
        }));
      }
    }

    map.put(commit.operation().key(), commit);
    return isActive(previous, commit.time()) ? previous.operation().value() : null;
  }

  /**
   * Handles a put if absent commit.
   */
  protected Object putIfAbsent(Commit<MapCommands.PutIfAbsent> commit) {
    if (map == null) {
      map = new HashMap<>();
    }

    Commit<? extends MapCommands.TtlCommand> previous = map.get(commit.operation().key());
    if (!isActive(previous, commit.time())) {
      if (previous == null) {
        if (commit.operation().ttl() > 0) {
          timers.put(commit.operation().key(), executor.schedule(() -> {
            map.remove(commit.operation().key());
            Listener<Session> listener = listeners.remove(commit.operation().key());
            if (listener != null)
              listener.close();
            commit.clean();
          }, Duration.ofMillis(commit.operation().ttl())));
        }

        if (commit.operation().mode() == PersistenceMode.EPHEMERAL) {
          listeners.put(commit.operation().key(), commit.session().onClose(s -> {
            map.remove(commit.operation().key());
            Scheduled scheduled = timers.remove(commit.operation().key());
            if (scheduled != null)
              scheduled.cancel();
            commit.clean();
          }));
        }
      }

      map.put(commit.operation().key(), commit);
      return null;
    }
    return previous.operation().value();
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
        commit.close();
      } else {
        Object value = previous.operation().value();
        if ((value == null && commit.operation().value() == null) || (value != null && commit.operation().value() != null && value.equals(commit.operation().value()))) {
          map.remove(commit.operation().key());
          previous.clean();
          commit.close();
          return true;
        }
        commit.clean();
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
        commit.close();
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
