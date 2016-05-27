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
package io.atomix.collections.internal;

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.copycat.server.Commit;
import io.atomix.resource.ResourceStateMachine;

import java.time.Duration;
import java.util.*;

/**
 * Map state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MapState extends ResourceStateMachine {
  private final Map<Object, Value> map = new HashMap<>();

  public MapState(Properties config) {
    super(config);
  }

  /**
   * Handles a contains key commit.
   */
  public boolean containsKey(Commit<MapCommands.ContainsKey> commit) {
    try {
      return map.containsKey(commit.operation().key());
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a contains value commit.
   */
  public boolean containsValue(Commit<MapCommands.ContainsValue> commit) {
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
  public Object get(Commit<MapCommands.Get> commit) {
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
  public Object getOrDefault(Commit<MapCommands.GetOrDefault> commit) {
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
  public Object put(Commit<MapCommands.Put> commit) {
    try {
      Scheduled timer = commit.operation().ttl() > 0 ? executor.schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
        map.remove(commit.operation().key()).commit.close();
      }) : null;

      Value value = map.put(commit.operation().key(), new Value(commit, timer));
      if (value != null) {
        try {
          if (value.timer != null)
            value.timer.cancel();
          return value.commit.operation().value();
        } finally {
          value.commit.close();
        }
      }
      return null;
    } catch (Exception e) {
      commit.close();
      throw e;
    }
  }

  /**
   * Handles a put if absent commit.
   */
  public Object putIfAbsent(Commit<MapCommands.PutIfAbsent> commit) {
    try {
      Value value = map.get(commit.operation().key());
      if (value == null) {
        Scheduled timer = commit.operation().ttl() > 0 ? executor.schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
          map.remove(commit.operation().key()).commit.close();
        }) : null;

        map.put(commit.operation().key(), new Value(commit, timer));
        return null;
      } else {
        commit.close();
        return value.commit.operation().value();
      }
    } catch (Exception e) {
      commit.close();
      throw e;
    }
  }

  /**
   * Handles a remove commit.
   */
  public Object remove(Commit<MapCommands.Remove> commit) {
    try {
      Value value = map.remove(commit.operation().key());
      if (value != null) {
        try {
          if (value.timer != null)
            value.timer.cancel();
          return value.commit.operation().value();
        } finally {
          value.commit.close();
        }
      }
      return null;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a remove if present commit.
   */
  public boolean removeIfPresent(Commit<MapCommands.RemoveIfPresent> commit) {
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
          value.commit.close();
        }
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a replace commit.
   */
  public Object replace(Commit<MapCommands.Replace> commit) {
    Value value = map.get(commit.operation().key());
    if (value != null) {
      try {
        if (value.timer != null)
          value.timer.cancel();
        Scheduled timer = commit.operation().ttl() > 0 ? executor.schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
          map.remove(commit.operation().key());
          commit.close();
        }) : null;
        map.put(commit.operation().key(), new Value(commit, timer));
        return value.commit.operation().value();
      } finally {
        value.commit.close();
      }
    } else {
      commit.close();
    }
    return null;
  }

  /**
   * Handles a replace if present commit.
   */
  public boolean replaceIfPresent(Commit<MapCommands.ReplaceIfPresent> commit) {
    Value value = map.get(commit.operation().key());
    if (value == null) {
      commit.close();
      return false;
    }

    if ((value.commit.operation().value() == null && commit.operation().replace() == null)
      || (value.commit.operation().value() != null && value.commit.operation().value().equals(commit.operation().replace()))) {
      if (value.timer != null)
        value.timer.cancel();
      Scheduled timer = commit.operation().ttl() > 0 ? executor.schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
        map.remove(commit.operation().key()).commit.close();
      }) : null;
      map.put(commit.operation().key(), new Value(commit, timer));
      value.commit.close();
      return true;
    } else {
      commit.close();
    }
    return false;
  }

  /**
   * Handles a values query.
   */
  public Collection<Object> values(Commit<MapCommands.Values> commit) {
    try {
      Collection<Object> values = new ArrayList<>();
      for (Value value : map.values()) {
        values.add(value.commit.operation().value());
      }
      return values;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a key set query.
   */
  public Set<Object> keySet(Commit<MapCommands.KeySet> commit) {
    try {
      return new HashSet<>(map.keySet());
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an entry set query.
   */
  public Set<Map.Entry<Object, Object>> entrySet(Commit<MapCommands.EntrySet> commit) {
    try {
      Set<Map.Entry<Object, Object>> entries = new HashSet<>();
      for (Map.Entry<Object, Value> entry : map.entrySet()) {
        entries.add(new MapEntry(entry.getKey(), entry.getValue().commit.operation().value()));
      }
      return entries;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a count commit.
   */
  public int size(Commit<MapCommands.Size> commit) {
    try {
      return map.size();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an is empty commit.
   */
  public boolean isEmpty(Commit<MapCommands.IsEmpty> commit) {
    try {
      return map == null || map.isEmpty();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a clear commit.
   */
  public void clear(Commit<MapCommands.Clear> commit) {
    try {
      delete();
    } finally {
      commit.close();
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
      value.commit.close();
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

  /**
   * Map entry.
   */
  private static class MapEntry implements Map.Entry<Object, Object> {
    private final Object key;
    private Object value;

    private MapEntry(Object key, Object value) {
      this.key = Assert.notNull(key, "key");
      this.value = value;
    }

    @Override
    public Object getKey() {
      return key;
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public Object setValue(Object value) {
      Object oldValue = this.value;
      this.value = value;
      return oldValue;
    }
  }

}
