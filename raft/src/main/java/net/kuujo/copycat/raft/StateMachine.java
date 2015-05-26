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
package net.kuujo.copycat.raft;

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.cluster.Session;
import net.kuujo.copycat.raft.storage.compact.Compaction;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Raft state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class StateMachine {
  private final Map<Class<? extends Command>, Method> filters = new HashMap<>();
  private Method allFilter;
  private final Map<Class<? extends Operation>, Method> operations = new HashMap<>();
  private Method allOperation;

  protected StateMachine() {
    init();
  }

  /**
   * Initializes the state machine.
   */
  private void init() {
    for (Method method : getClass().getMethods()) {
      Filter filter = method.getAnnotation(Filter.class);
      if (filter != null) {
        if (method.getReturnType() != Boolean.class && method.getReturnType() != boolean.class) {
          throw new ConfigurationException("filter method " + method + " must return boolean");
        }

        for (Class<? extends Command> command : filter.value()) {
          if (command == Filter.All.class) {
            allFilter = method;
          } else {
            filters.put(command, method);
          }
        }
      }

      Apply apply = method.getAnnotation(Apply.class);
      if (apply != null) {
        for (Class<? extends Operation> command : apply.value()) {
          if (command == Apply.All.class) {
            allOperation = method;
          } else {
            operations.put(command, method);
          }
        }
      }
    }
  }

  /**
   * Called when a new session is registered.
   *
   * @param session The session that was registered.
   */
  public void register(Session session) {

  }

  /**
   * Filters a command.
   *
   * @param commit The commit to filter.
   * @param compaction The compaction context.
   * @return Whether to keep the commit.
   */
  public boolean filter(Commit<? extends Command> commit, Compaction compaction) {
    Method method = filters.get(commit.type());
    if (method == null) {
      method = allFilter;
      if (method == null) {
        throw new IllegalArgumentException("unknown command type: " + commit.type());
      }
    }

    try {
      return (boolean) method.invoke(this, commit);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new ApplicationException("failed to filter command", e);
    }
  }

  /**
   * Applies an operation to the state machine.
   *
   * @param commit The commit to apply.
   * @return The operation result.
   */
  public Object apply(Commit<? extends Operation> commit) {
    Method method = operations.get(commit.type());
    if (method == null) {
      method = allOperation;
      if (method == null) {
        throw new IllegalArgumentException("unknown operation type:" + commit.type());
      }
    }

    try {
      return method.invoke(this, commit);
    } catch (IllegalAccessException | InvocationTargetException e) {
      return new ApplicationException("failed to invoke operation", e);
    }
  }

  /**
   * Called when a session is expired.
   *
   * @param session The expired session.
   */
  public void expire(Session session) {

  }

  /**
   * Called when a session is closed.
   *
   * @param session The session that was closed.
   */
  public void close(Session session) {

  }

}
