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
package net.kuujo.copycat.resource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Resource state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class StateMachine {
  private final Map<Class<? extends Command>, Method> commands = new HashMap<>();
  private final Map<Class<? extends Command>, Method> filters = new HashMap<>();
  private Method allCommand;
  private Method allFilter;

  protected StateMachine() {
    init();
  }

  /**
   * Initializes the state machine.
   */
  private void init() {
    for (Method method : getClass().getMethods()) {
      Apply apply = method.getAnnotation(Apply.class);
      if (apply != null) {
        for (Class<? extends Command> command : apply.value()) {
          if (command == Apply.All.class) {
            allCommand = method;
          } else {
            commands.put(command, method);
          }
        }
      }

      Filter filter = method.getAnnotation(Filter.class);
      if (filter != null) {
        for (Class<? extends Command> command : filter.value()) {
          if (command == Filter.All.class) {
            allFilter = method;
          } else {
            filters.put(command, method);
          }
        }
      }
    }
  }

  /**
   * Applies a command to the state machine.
   *
   * @param commit The commit to handle.
   * @return The command result.
   */
  Object apply(Commit<?> commit) {
    Method method = commands.get(commit.type());
    if (method == null) {
      method = allCommand;
      if (method == null) {
        throw new IllegalArgumentException("unknown command type:" + commit.type());
      }
    }

    try {
      return method.invoke(this, commit);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new ResourceException("failed to invoke command", e);
    }
  }

  /**
   * Filters a command on behalf of the state machine.
   *
   * @param commit The commit to filter.
   * @return Indicates whether to remove the command from the log.
   */
  boolean filter(Commit<?> commit) {
    Method method = filters.get(commit.type());
    if (method == null) {
      method = allFilter;
    }

    if (method != null) {
      try {
        return (boolean) method.invoke(this, commit);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new ResourceException("failed to invoke command", e);
      }
    }
    return false;
  }

}
