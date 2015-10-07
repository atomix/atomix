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
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;

import java.time.Duration;
import java.util.*;

/**
 * Map state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MultiMapState extends StateMachine {
  private final Map<Object, Map<Object, Commit<? extends MultiMapCommands.TtlCommand>>> map = new HashMap<>();
  private final Map<Long, Scheduled> timers = new HashMap<>();

  @Override
  protected void configure(StateMachineExecutor executor) {
    executor.register(MultiMapCommands.ContainsKey.class, this::containsKey);
    executor.register(MultiMapCommands.Get.class, this::get);
    executor.register(MultiMapCommands.Put.class, this::put);
    executor.register(MultiMapCommands.Remove.class, this::remove);
    executor.register(MultiMapCommands.Size.class, this::size);
    executor.register(MultiMapCommands.IsEmpty.class, this::isEmpty);
    executor.register(MultiMapCommands.Clear.class, this::clear);
  }

  /**
   * Handles a contains key commit.
   */
  protected boolean containsKey(Commit<MultiMapCommands.ContainsKey> commit) {
    try {
      return map.containsKey(commit.operation().key());
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a get commit.
   */
  protected Collection get(Commit<MultiMapCommands.Get> commit) {
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
  protected boolean put(Commit<MultiMapCommands.Put> commit) {
    try {
      Map<Object, Commit<? extends MultiMapCommands.TtlCommand>> values = map.get(commit.operation().key());
      if (values == null) {
        values = new LinkedHashMap<>();
        map.put(commit.operation().key(), values);
      }

      final Map<Object, Commit<? extends MultiMapCommands.TtlCommand>> keyValues = values;
      if (!values.containsKey(commit.operation().value())) {
        Scheduled timer = commit.operation().ttl() > 0 ? executor().schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
          keyValues.remove(commit.operation().value()).clean();
        }) : null;
        timers.put(commit.index(), timer);
        return true;
      } else {
        commit.clean();
        return false;
      }
    } catch (Exception e) {
      commit.clean();
      throw e;
    }
  }

  /**
   * Handles a remove commit.
   */
  protected Object remove(Commit<MultiMapCommands.Remove> commit) {
    try {
      if (commit.operation().value() != null) {
        Map<Object, Commit<? extends MultiMapCommands.TtlCommand>> values = map.get(commit.operation().key());
        if (values == null) {
          return false;
        }

        Commit<? extends MultiMapCommands.TtlCommand> previous = values.remove(commit.operation().value());
        if (previous == null) {
          return false;
        } else {
          previous.clean();
        }

        Scheduled timer = timers.remove(previous.index());
        if (timer != null)
          timer.cancel();

        if (values.isEmpty())
          map.remove(commit.operation().key());
        return true;
      } else {
        Map<Object, Commit<? extends MultiMapCommands.TtlCommand>> values = map.remove(commit.operation().key());
        if (values != null) {
          Collection<Object> results = new ArrayList<>();
          for (Commit<? extends MultiMapCommands.TtlCommand> value : values.values()) {
            Scheduled timer = timers.remove(value.index());
            if (timer != null)
              timer.cancel();
            results.add(value.operation().value());
          }
          return results;
        }
        return Collections.EMPTY_LIST;
      }
    } finally {
      commit.clean();
    }
  }

  /**
   * Handles a count commit.
   */
  protected int size(Commit<MultiMapCommands.Size> commit) {
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
  protected boolean isEmpty(Commit<MultiMapCommands.IsEmpty> commit) {
    try {
      return map == null || map.isEmpty();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a clear commit.
   */
  protected void clear(Commit<MultiMapCommands.Clear> commit) {
    try {
      Iterator<Map.Entry<Object, Map<Object, Commit<? extends MultiMapCommands.TtlCommand>>>> iterator = map.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Object, Map<Object, Commit<? extends MultiMapCommands.TtlCommand>>> entry = iterator.next();
        for (Commit<? extends MultiMapCommands.TtlCommand> value : entry.getValue().values()) {
          Scheduled timer = timers.remove(value.index());
          if (timer != null)
            timer.cancel();
          value.clean();
        }
        iterator.remove();
      }
    } finally {
      commit.clean();
    }
  }

}
