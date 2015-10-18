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
package io.atomix.collections.state;

import io.atomix.catalyst.util.concurrent.Scheduled;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.resource.ResourceStateMachine;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Map state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MapState extends ResourceStateMachine {
  private final Map<Object, Value> map = new HashMap<>();

  @Override
  protected void configure(StateMachineExecutor executor) {
    executor.register(MapCommands.ContainsKey.class, this::containsKey);
    executor.register(MapCommands.ContainsValue.class, this::containsValue);
    executor.register(MapCommands.Get.class, this::get);
    executor.register(MapCommands.GetOrDefault.class, this::getOrDefault);
    executor.register(MapCommands.Put.class, this::put);
    executor.register(MapCommands.PutIfAbsent.class, this::putIfAbsent);
    executor.register(MapCommands.Remove.class, this::remove);
    executor.register(MapCommands.RemoveIfPresent.class, this::removeIfPresent);
    executor.register(MapCommands.Replace.class, this::replace);
    executor.register(MapCommands.ReplaceIfPresent.class, this::replaceIfPresent);
    executor.register(MapCommands.Size.class, this::size);
    executor.register(MapCommands.IsEmpty.class, this::isEmpty);
    executor.register(MapCommands.Clear.class, this::clear);
  }

  /**
   * Handles a contains key commit.
   */
  protected boolean containsKey(Commit<MapCommands.ContainsKey> commit) {
    try {
      return map.containsKey(commit.operation().key());
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a contains value commit.
   */
  protected boolean containsValue(Commit<MapCommands.ContainsValue> commit) {
    try {
      for (Value value : map.values()) {
        if (value.commit.operation().value().equals(commit.operation().value())) {
          return true;
        }
      }
      return false;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get commit.
   */
  protected Object get(Commit<MapCommands.Get> commit) {
    try {
      Value value = map.get(commit.operation().key());
      return value != null ? value.commit.operation().value() : null;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get or default commit.
   */
  protected Object getOrDefault(Commit<MapCommands.GetOrDefault> commit) {
    try {
      Value value = map.get(commit.operation().key());
      return value != null ? value.commit.operation().value() : commit.operation().defaultValue();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a put commit.
   */
  protected Object put(Commit<MapCommands.Put> commit) {
    try {
      Scheduled timer = commit.operation().ttl() > 0 ? executor().schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
        map.remove(commit.operation().key()).commit.clean();
      }) : null;

      Value value = map.put(commit.operation().key(), new Value(commit, timer));
      if (value != null) {
        try {
          if (value.timer != null)
            value.timer.cancel();
          return value.commit.operation().value();
        } finally {
          value.commit.clean();
        }
      }
      return null;
    } catch (Exception e) {
      commit.clean();
      throw e;
    }
  }

  /**
   * Handles a put if absent commit.
   */
  protected Object putIfAbsent(Commit<MapCommands.PutIfAbsent> commit) {
    try {
      Value value = map.get(commit.operation().key());
      if (value == null) {
        Scheduled timer = commit.operation().ttl() > 0 ? executor().schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
          map.remove(commit.operation().key()).commit.clean();
        }) : null;

        map.put(commit.operation().key(), new Value(commit, timer));
        return null;
      } else {
        commit.clean();
        return value.commit.operation().value();
      }
    } catch (Exception e) {
      commit.clean();
      throw e;
    }
  }

  /**
   * Handles a remove commit.
   */
  protected Object remove(Commit<MapCommands.Remove> commit) {
    try {
      Value value = map.remove(commit.operation().key());
      if (value != null) {
        try {
          if (value.timer != null)
            value.timer.cancel();
          return value.commit.operation().value();
        } finally {
          value.commit.clean();
        }
      }
      return null;
    } finally {
      commit.clean();
    }
  }

  /**
   * Handles a remove if present commit.
   */
  protected boolean removeIfPresent(Commit<MapCommands.RemoveIfPresent> commit) {
    try {
      Value value = map.get(commit.operation().key());
      if (value == null || ((value.commit.operation().value() == null && commit.operation().value() != null)
        || (value.commit.operation().value() != null && !value.commit.operation().value().equals(commit.operation().value())))) {
        return false;
      } else {
        try {
          map.remove(commit.operation().key());
          if (value.timer != null)
            value.timer.cancel();
          return true;
        } finally {
          value.commit.clean();
        }
      }
    } finally {
      commit.clean();
    }
  }

  /**
   * Handles a replace commit.
   */
  protected Object replace(Commit<MapCommands.Replace> commit) {
    Value value = map.get(commit.operation().key());
    if (value != null) {
      try {
        if (value.timer != null)
          value.timer.cancel();
        Scheduled timer = commit.operation().ttl() > 0 ? executor().schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
          map.remove(commit.operation().key());
          commit.clean();
        }) : null;
        map.put(commit.operation().key(), new Value(commit, timer));
        return value.commit.operation().value();
      } finally {
        value.commit.clean();
      }
    } else {
      commit.clean();
    }
    return null;
  }

  /**
   * Handles a replace if present commit.
   */
  protected boolean replaceIfPresent(Commit<MapCommands.ReplaceIfPresent> commit) {
    Value value = map.get(commit.operation().key());
    if (value == null) {
      commit.clean();
      return false;
    }

    if ((value.commit.operation().value() == null && commit.operation().replace() == null)
      || (value.commit.operation().value() != null && value.commit.operation().value().equals(commit.operation().replace()))) {
      if (value.timer != null)
        value.timer.cancel();
      Scheduled timer = commit.operation().ttl() > 0 ? executor().schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
        map.remove(commit.operation().key()).commit.clean();
      }) : null;
      map.put(commit.operation().key(), new Value(commit, timer));
      value.commit.clean();
      return true;
    } else {
      commit.clean();
    }
    return false;
  }

  /**
   * Handles a count commit.
   */
  protected int size(Commit<MapCommands.Size> commit) {
    try {
      return map.size();
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
    try {
      delete();
    } finally {
      commit.clean();
    }
  }

  @Override
  public void delete() {
    Iterator<Map.Entry<Object, Value>> iterator = map.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Object, Value> entry = iterator.next();
      Value value = entry.getValue();
      if (value.timer != null)
        value.timer.cancel();
      value.commit.clean();
      iterator.remove();
    }
  }

  /**
   * Map value.
   */
  private static class Value {
    private final Commit<? extends MapCommands.TtlCommand> commit;
    private final Scheduled timer;

    private Value(Commit<? extends MapCommands.TtlCommand> commit, Scheduled timer) {
      this.commit = commit;
      this.timer = timer;
    }
  }

}
