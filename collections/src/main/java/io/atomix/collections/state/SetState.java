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
import io.atomix.resource.ResourceStateMachine;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Distributed set state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SetState extends ResourceStateMachine {
  private final Map<Object, Value> map = new HashMap<>();

  /**
   * Handles a contains commit.
   */
  public boolean contains(Commit<SetCommands.Contains> commit) {
    try {
      return map.containsKey(commit.operation().value());
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an add commit.
   */
  public boolean add(Commit<SetCommands.Add> commit) {
    try {
      Value value = map.get(commit.operation().value());
      if (value == null) {
        Scheduled timer = commit.operation().ttl() > 0 ? executor().schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
          map.remove(commit.operation().value()).commit.clean();
        }) : null;
        map.put(commit.operation().value(), new Value(commit, timer));
        return true;
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
  public boolean remove(Commit<SetCommands.Remove> commit) {
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
  public int size(Commit<SetCommands.Size> commit) {
    try {
      return map != null ? map.size() : 0;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles an is empty commit.
   */
  public boolean isEmpty(Commit<SetCommands.IsEmpty> commit) {
    try {
      return map == null || map.isEmpty();
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a clear commit.
   */
  public void clear(Commit<SetCommands.Clear> commit) {
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
