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
import net.kuujo.copycat.raft.log.Compaction;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.server.Apply;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.Filter;
import net.kuujo.copycat.raft.server.StateMachine;

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
  private final Map<Object, Commit<? extends MapCommands.TtlCommand>> map = new HashMap<>();
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
  private boolean isActive(Commit<? extends MapCommands.TtlCommand> commit) {
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
   * Handles a contains key commit.
   */
  @Apply(MapCommands.ContainsKey.class)
  protected boolean containsKey(Commit<MapCommands.ContainsKey> commit) {
    updateTime(commit);
    Commit<? extends MapCommands.TtlCommand> command = map.get(commit.operation().key());
    if (!isActive(command)) {
      map.remove(commit.operation().key());
      return false;
    }
    return true;
  }

  /**
   * Handles a get commit.
   */
  @Apply(MapCommands.Get.class)
  protected Object get(Commit<MapCommands.Get> commit) {
    updateTime(commit);
    Commit<? extends MapCommands.TtlCommand> command = map.get(commit.operation().key());
    if (command != null) {
      if (!isActive(command)) {
        map.remove(commit.operation().key());
      } else {
        return command.operation().value();
      }
    }
    return null;
  }

  /**
   * Handles a get or default commit.
   */
  @Apply(MapCommands.GetOrDefault.class)
  protected Object getOrDefault(Commit<MapCommands.GetOrDefault> commit) {
    updateTime(commit);
    Commit<? extends MapCommands.TtlCommand> command = map.get(commit.operation().key());
    if (command == null) {
      return commit.operation().defaultValue();
    } else if (!isActive(command)) {
      map.remove(commit.operation().key());
    } else {
      return command.operation().value();
    }
    return commit.operation().defaultValue();
  }

  /**
   * Handles a put commit.
   */
  @Apply(MapCommands.Put.class)
  protected Object put(Commit<MapCommands.Put> commit) {
    updateTime(commit);
    Commit<? extends MapCommands.TtlCommand> command = map.put(commit.operation().key(), commit);
    return isActive(command) ? command.operation().value : null;
  }

  /**
   * Handles a put if absent commit.
   */
  @Apply(MapCommands.PutIfAbsent.class)
  protected Object putIfAbsent(Commit<MapCommands.PutIfAbsent> commit) {
    updateTime(commit);
    Commit<? extends MapCommands.TtlCommand> command = map.putIfAbsent(commit.operation().key(), commit);
    return isActive(command) ? command.operation().value : null;
  }

  /**
   * Filters a put and put if absent commit.
   */
  @Filter({MapCommands.Put.class, MapCommands.PutIfAbsent.class})
  protected boolean filterPut(Commit<? extends MapCommands.TtlCommand> commit) {
    Commit<? extends MapCommands.TtlCommand> command = map.get(commit.operation().key());
    return isActive(command) && command.index() == commit.index();
  }

  /**
   * Handles a remove commit.
   */
  @Apply(MapCommands.Remove.class)
  protected Object remove(Commit<MapCommands.Remove> commit) {
    updateTime(commit);
    if (commit.operation().value() != null) {
      Commit<? extends MapCommands.TtlCommand> command = map.get(commit.operation().key());
      if (!isActive(command)) {
        map.remove(commit.operation().key());
      } else {
        Object value = command.operation().value();
        if ((value == null && commit.operation().value() == null) || (value != null && commit.operation().value() != null && value.equals(commit.operation().value()))) {
          map.remove(commit.operation().key());
          return true;
        }
        return false;
      }
      return false;
    } else {
      Commit<? extends MapCommands.TtlCommand> command =  map.remove(commit.operation().key());
      return isActive(command) ? command.operation().value() : null;
    }
  }

  /**
   * Filters a remove commit.
   */
  @Filter(value={MapCommands.Remove.class, MapCommands.Clear.class}, compaction= Compaction.Type.MAJOR)
  protected boolean filterRemove(Commit<?> commit, Compaction compaction) {
    return commit.index() > compaction.index();
  }

  /**
   * Handles a size commit.
   */
  @Apply(MapCommands.Size.class)
  protected int size(Commit<MapCommands.Size> commit) {
    updateTime(commit);
    return map.size();
  }

  /**
   * Handles an is empty commit.
   */
  @Apply(MapCommands.IsEmpty.class)
  protected boolean isEmpty(Commit<MapCommands.IsEmpty> commit) {
    updateTime(commit);
    return map.isEmpty();
  }

  /**
   * Handles a clear commit.
   */
  @Apply(MapCommands.Clear.class)
  protected void clear(Commit<MapCommands.Clear> commit) {
    updateTime(commit);
    map.clear();
  }

}
