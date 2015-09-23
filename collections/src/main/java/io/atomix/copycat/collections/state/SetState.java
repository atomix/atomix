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
package io.atomix.copycat.collections.state;

import io.atomix.catalog.server.Commit;
import io.atomix.catalog.server.StateMachine;
import io.atomix.catalog.server.StateMachineExecutor;
import io.atomix.catalyst.util.concurrent.Scheduled;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Distributed set state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SetState extends StateMachine {
  private final Map<Integer, Value> map = new HashMap<>();

  @Override
  protected void configure(StateMachineExecutor executor) {
    executor.register(SetCommands.Add.class, this::add);
    executor.register(SetCommands.Contains.class, this::contains);
    executor.register(SetCommands.Remove.class, this::remove);
    executor.register(SetCommands.Size.class, this::size);
    executor.register(SetCommands.IsEmpty.class, this::isEmpty);
    executor.register(SetCommands.Clear.class, this::clear);
  }

  /**
   * Handles a contains commit.
   */
  protected boolean contains(Commit<SetCommands.Contains> commit) {
    try {
      return map.containsKey(commit.operation().value());
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an add commit.
   */
  protected boolean add(Commit<SetCommands.Add> commit) {
    try {
      Value value = map.get(commit.operation().value());
      if (value == null) {
        Scheduled timer = commit.operation().ttl() > 0 ? executor().schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
          map.remove(commit.operation().value()).commit.clean();
        }) : null;
        map.put(commit.operation().value(), new Value(commit, timer));
      } else {
        commit.clean();
      }
    } catch (Exception e) {
      commit.clean();
      throw e;
    }
    return false;
  }

  /**
   * Handles a remove commit.
   */
  protected boolean remove(Commit<SetCommands.Remove> commit) {
    try {
      Value value = map.remove(commit.operation().value());
      if (value != null) {
        try {
          if (value.timer != null) {
            value.timer.cancel();
          }
          return true;
        } finally {
          value.commit.clean();
        }
      }
      return false;
    } finally {
      commit.clean();
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
    try {
      Iterator<Map.Entry<Integer, Value>> iterator = map.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, Value> entry = iterator.next();
        Value value = entry.getValue();
        if (value.timer != null)
          value.timer.cancel();
        value.commit.clean(true);
        iterator.remove();
      }
    } finally {
      commit.clean();
    }
  }

  /**
   * Set value.
   */
  private static class Value {
    private final Commit<? extends SetCommands.TtlCommand> commit;
    private final Scheduled timer;

    private Value(Commit<? extends SetCommands.TtlCommand> commit, Scheduled timer) {
      this.commit = commit;
      this.timer = timer;
    }
  }

}
