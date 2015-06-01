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
import net.kuujo.copycat.raft.log.Compaction;

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
    init(getClass());

    for (Method method : getClass().getMethods()) {
      declareFilters(method);
      declareOperations(method);
    }
  }

  /**
   * Initializes the state machine.
   */
  private void init(Class<?> clazz) {
    while (clazz != Object.class) {
      for (Method method : clazz.getDeclaredMethods()) {
        declareFilters(method);
        declareOperations(method);
      }

      for (Class<?> iface : clazz.getInterfaces()) {
        init(iface);
      }
      clazz = clazz.getSuperclass();
    }
  }

  /**
   * Declares any filters defined by the given method.
   */
  private void declareFilters(Method method) {
    Filter filter = method.getAnnotation(Filter.class);
    if (filter != null) {
      if (method.getReturnType() != Boolean.class && method.getReturnType() != boolean.class) {
        throw new ConfigurationException("filter method " + method + " must return boolean");
      }

      method.setAccessible(true);
      for (Class<? extends Command> command : filter.value()) {
        if (command == Filter.All.class) {
          allFilter = method;
        } else if (!filters.containsKey(command)) {
          filters.put(command, method);
        }
      }
    }
  }

  /**
   * Finds the filter method for the given command.
   */
  private Method findFilter(Class<? extends Command> type) {
    Method method = filters.computeIfAbsent(type, t -> {
      for (Map.Entry<Class<? extends Command>, Method> entry : filters.entrySet()) {
        if (entry.getKey().isAssignableFrom(type)) {
          return entry.getValue();
        }
      }
      return allFilter;
    });

    if (method == null) {
      throw new IllegalArgumentException("unknown command type: " + type);
    }
    return method;
  }

  /**
   * Declares any operations defined by the given method.
   */
  private void declareOperations(Method method) {
    Apply apply = method.getAnnotation(Apply.class);
    if (apply != null) {
      method.setAccessible(true);
      for (Class<? extends Operation> operation : apply.value()) {
        if (operation == Apply.All.class) {
          allOperation = method;
        } else if (!operations.containsKey(operation)) {
          operations.put(operation, method);
        }
      }
    }
  }

  /**
   * Finds the operation method for the given operation.
   */
  private Method findOperation(Class<? extends Operation> type) {
    Method method = operations.computeIfAbsent(type, t -> {
      for (Map.Entry<Class<? extends Operation>, Method> entry : operations.entrySet()) {
        if (entry.getKey().isAssignableFrom(type)) {
          return entry.getValue();
        }
      }
      return allOperation;
    });

    if (method == null) {
      throw new IllegalArgumentException("unknown operation type: " + type);
    }
    return method;
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
    try {
      return (boolean) findFilter(commit.type()).invoke(this, commit);
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
    try {
      return findOperation(commit.type()).invoke(this, commit);
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
