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
import net.kuujo.copycat.log.Compaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * Raft state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class StateMachine {
  private final Logger LOGGER = LoggerFactory.getLogger(getClass());
  private final Map<Compaction.Type, Map<Class<? extends Command>, BiPredicate<Commit<?>, Compaction>>> filters = new HashMap<>();
  private Map<Compaction.Type, BiPredicate<Commit<?>, Compaction>> allFilters = new HashMap<>();
  private final Map<Class<? extends Operation>, Function<Commit<?>, ?>> operations = new HashMap<>();
  private Function<Commit<?>, ?> allOperation;

  protected StateMachine() {
    init();
  }

  /**
   * Initializes the state machine.
   */
  private void init() {
    init(getClass());
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
          if (!allFilters.containsKey(filter.compaction())) {
            if (method.getParameterCount() == 1) {
              allFilters.put(filter.compaction(), wrapFilter(method));
            }
          }
        } else {
          Map<Class<? extends Command>, BiPredicate<Commit<?>, Compaction>> filters = this.filters.get(filter.compaction());
          if (filters == null) {
            filters = new HashMap<>();
            this.filters.put(filter.compaction(), filters);
          }
          if (!filters.containsKey(command)) {
            filters.put(command, wrapFilter(method));
          }
        }
      }
    }
  }

  /**
   * Wraps a filter method.
   */
  private BiPredicate<Commit<?>, Compaction> wrapFilter(Method method) {
    if (method.getParameterCount() == 1) {
      return (commit, compaction) -> {
        try {
          return (boolean) method.invoke(this, commit);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new ApplicationException("failed to filter command", e);
        }
      };
    } else if (method.getParameterCount() == 2) {
      return (commit, compaction) -> {
        try {
          return (boolean) method.invoke(this, commit, compaction);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new ApplicationException("failed to filter command", e);
        }
      };
    } else {
      throw new IllegalStateException("invalid filter method: too many parameters");
    }
  }

  /**
   * Finds the filter method for the given command.
   */
  private BiPredicate<Commit<?>, Compaction> findFilter(Class<? extends Command> type, Compaction.Type compaction) {
    Map<Class<? extends Command>, BiPredicate<Commit<?>, Compaction>> filters = this.filters.get(compaction);
    if (filters == null) {
      BiPredicate<Commit<?>, Compaction> filter = allFilters.get(compaction);
      if (filter == null) {
        throw new IllegalArgumentException("unknown command type: " + type);
      }
      return filter;
    }

    BiPredicate<Commit<?>, Compaction> filter = filters.computeIfAbsent(type, t -> {
      for (Map.Entry<Class<? extends Command>, BiPredicate<Commit<?>, Compaction>> entry : filters.entrySet()) {
        if (entry.getKey().isAssignableFrom(type)) {
          return entry.getValue();
        }
      }
      return allFilters.get(compaction);
    });

    if (filter == null) {
      throw new IllegalArgumentException("unknown command type: " + type);
    }
    return filter;
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
          allOperation = wrapOperation(method);
        } else if (!operations.containsKey(operation)) {
          operations.put(operation, wrapOperation(method));
        }
      }
    }
  }

  /**
   * Wraps an operation method.
   */
  private Function<Commit<?>, ?> wrapOperation(Method method) {
    if (method.getParameterCount() < 1) {
      throw new IllegalStateException("invalid operation method: not enough arguments");
    } else if (method.getParameterCount() > 1) {
      throw new IllegalStateException("invalid operation method: too many arguments");
    } else {
      return commit -> {
        try {
          return method.invoke(this, commit);
        } catch (IllegalAccessException | InvocationTargetException e) {
          return new ApplicationException("failed to invoke operation", e);
        }
      };
    }
  }

  /**
   * Finds the operation method for the given operation.
   */
  private Function<Commit<?>, ?> findOperation(Class<? extends Operation> type) {
    Function<Commit<?>, ?> operation = operations.computeIfAbsent(type, t -> {
      for (Map.Entry<Class<? extends Operation>, Function<Commit<?>, ?>> entry : operations.entrySet()) {
        if (entry.getKey().isAssignableFrom(type)) {
          return entry.getValue();
        }
      }
      return allOperation;
    });

    if (operation == null) {
      throw new IllegalArgumentException("unknown operation type: " + type);
    }
    return operation;
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
    LOGGER.debug("Filtering {}", commit);
    return findFilter(commit.type(), compaction.type()).test(commit, compaction);
  }

  /**
   * Applies an operation to the state machine.
   *
   * @param commit The commit to apply.
   * @return The operation result.
   */
  public Object apply(Commit<? extends Operation> commit) {
    LOGGER.debug("Applying {}", commit);
    return findOperation(commit.type()).apply(commit);
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
