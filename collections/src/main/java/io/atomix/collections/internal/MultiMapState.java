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

import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.collections.DistributedMultiMap;
import io.atomix.copycat.server.Commit;
import io.atomix.resource.ResourceStateMachine;

import java.time.Duration;
import java.util.*;

/**
 * Map state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MultiMapState extends ResourceStateMachine {
  private final Map<Object, Map<Object, Commit<? extends MultiMapCommands.TtlCommand>>> map = new HashMap<>();
  private final Map<Long, Scheduled> timers = new HashMap<>();
  private final DistributedMultiMap.Order order;

  public MultiMapState(Properties properties) {
    super(properties);
    this.order = DistributedMultiMap.Order.valueOf(config.getProperty("order", DistributedMultiMap.Order.INSERT.name().toLowerCase()).toUpperCase());
  }

  /**
   * Creates a new value map.
   */
  private Map<Object, Commit<? extends MultiMapCommands.TtlCommand>> createValueMap() {
    switch (order) {
      case NONE:
        return new HashMap<>();
      case NATURAL:
        return new TreeMap<>();
      case INSERT:
        return new LinkedHashMap<>();
      default:
        return new HashMap<>();
    }
  }

  /**
   * Handles a contains key commit.
   */
  public boolean containsKey(Commit<MultiMapCommands.ContainsKey> commit) {
    try {
      return map.containsKey(commit.operation().key());
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get commit.
   */
  public Collection get(Commit<MultiMapCommands.Get> commit) {
    try {
      Map<Object, Commit<? extends MultiMapCommands.TtlCommand>> values = map.get(commit.operation().key());
      if (values == null) {
        return Collections.EMPTY_LIST;
      }

      Collection<Object> results = new ArrayList<>(values.size());
      for (Commit<? extends MultiMapCommands.TtlCommand> value : values.values()) {
        results.add(value.operation().value());
      }
      return results;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a put commit.
   */
  public boolean put(Commit<MultiMapCommands.Put> commit) {
    try {
      Map<Object, Commit<? extends MultiMapCommands.TtlCommand>> values = map.get(commit.operation().key());
      if (values == null) {
        values = createValueMap();
        map.put(commit.operation().key(), values);
      }

      final Map<Object, Commit<? extends MultiMapCommands.TtlCommand>> keyValues = values;
      if (!values.containsKey(commit.operation().value())) {
        Scheduled timer = commit.operation().ttl() > 0 ? executor.schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
          keyValues.remove(commit.operation().value()).close();
        }) : null;
        values.put(commit.operation().value(), commit);
        timers.put(commit.index(), timer);
        return true;
      } else {
        commit.close();
        return false;
      }
    } catch (Exception e) {
      commit.close();
      throw e;
    }
  }

  /**
   * Handles a remove commit.
   */
  public Object remove(Commit<MultiMapCommands.Remove> commit) {
    try {
      if (commit.operation().value() != null) {
        Map<Object, Commit<? extends MultiMapCommands.TtlCommand>> values = map.get(commit.operation().key());
        if (values == null) {
          return false;
        }

        Commit<? extends MultiMapCommands.TtlCommand> previous = values.remove(commit.operation().value());
        if (previous == null) {
          return false;
        }

        Scheduled timer = timers.remove(previous.index());
        if (timer != null)
          timer.cancel();

        previous.close();

        if (values.isEmpty())
          map.remove(commit.operation().key());
        return true;
      } else {
        Map<Object, Commit<? extends MultiMapCommands.TtlCommand>> values = map.remove(commit.operation().key());
        if (values != null) {
          Collection<Object> results = new ArrayList<>(values.size());
          for (Commit<? extends MultiMapCommands.TtlCommand> value : values.values()) {
            Scheduled timer = timers.remove(value.index());
            if (timer != null)
              timer.cancel();
            results.add(value.operation().value());
            value.close();
          }
          return results;
        }
        return Collections.EMPTY_LIST;
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a remove value commit.
   */
  public void removeValue(Commit<MultiMapCommands.RemoveValue> commit) {
    try {
      Iterator<Map.Entry<Object, Map<Object, Commit<? extends MultiMapCommands.TtlCommand>>>> outerIterator = map.entrySet().iterator();
      while (outerIterator.hasNext()) {
        Map<Object, Commit<? extends MultiMapCommands.TtlCommand>> map = outerIterator.next().getValue();
        Iterator<Map.Entry<Object, Commit<? extends MultiMapCommands.TtlCommand>>> innerIterator = map.entrySet().iterator();
        while (innerIterator.hasNext()) {
          Map.Entry<Object, Commit<? extends MultiMapCommands.TtlCommand>> entry = innerIterator.next();
          if ((entry.getValue().operation().value() == null && commit.operation().value() == null)
            || (entry.getValue().operation().value() != null && commit.operation().value() != null && entry.getValue().operation().value().equals(commit.operation().value()))) {
            Scheduled timer = timers.remove(entry.getValue().index());
            if (timer != null)
              timer.cancel();
            entry.getValue().close();
            innerIterator.remove();
          }
        }

        if (map.isEmpty()) {
          outerIterator.remove();
        }
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a size commit.
   */
  public int size(Commit<MultiMapCommands.Size> commit) {
    try {
      if (commit.operation().key() != null) {
        Map<Object, Commit<? extends MultiMapCommands.TtlCommand>> values = map.get(commit.operation().key());
        return values != null ? values.size() : 0;
      } else {
        int size = 0;
        for (Map.Entry<Object, Map<Object, Commit<? extends MultiMapCommands.TtlCommand>>> entry : map.entrySet()) {
          size += entry.getValue().size();
        }
        return size;
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an is empty commit.
   */
  public boolean isEmpty(Commit<MultiMapCommands.IsEmpty> commit) {
    try {
      return map == null || map.isEmpty();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a clear commit.
   */
  public void clear(Commit<MultiMapCommands.Clear> commit) {
    try {
      delete();
    } finally {
      commit.close();
    }
  }

  @Override
  public void delete() {
    Iterator<Map.Entry<Object, Map<Object, Commit<? extends MultiMapCommands.TtlCommand>>>> iterator = map.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Object, Map<Object, Commit<? extends MultiMapCommands.TtlCommand>>> entry = iterator.next();
      for (Commit<? extends MultiMapCommands.TtlCommand> value : entry.getValue().values()) {
        Scheduled timer = timers.remove(value.index());
        if (timer != null)
          timer.cancel();
        value.close();
      }
      iterator.remove();
    }
  }

}
