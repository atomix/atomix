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
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Raft state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class StateMachine implements AutoCloseable {
  private final Logger LOGGER = LoggerFactory.getLogger(getClass());
  private final Map<Compaction.Type, Map<Class<? extends Command>, FilterExecutor>> filters = new HashMap<>();
  private Map<Compaction.Type, FilterExecutor> allFilters = new HashMap<>();
  private final Map<Class<? extends Operation>, OperationExecutor> operations = new HashMap<>();
  private OperationExecutor allOperation;

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
              allFilters.put(filter.compaction(), new FilterExecutor(method));
            }
          }
        } else {
          Map<Class<? extends Command>, FilterExecutor> filters = this.filters.get(filter.compaction());
          if (filters == null) {
            filters = new HashMap<>();
            this.filters.put(filter.compaction(), filters);
          }
          if (!filters.containsKey(command)) {
            filters.put(command, new FilterExecutor(method));
          }
        }
      }
    }
  }

  /**
   * Finds the filter method for the given command.
   */
  private BiFunction<Commit<?>, Compaction, CompletableFuture<Boolean>> findFilter(Class<? extends Command> type, Compaction.Type compaction) {
    Map<Class<? extends Command>, FilterExecutor> filters = this.filters.get(compaction);
    if (filters == null) {
      BiFunction<Commit<?>, Compaction, CompletableFuture<Boolean>> filter = allFilters.get(compaction);
      if (filter == null) {
        throw new IllegalArgumentException("unknown command type: " + type);
      }
      return filter;
    }

    BiFunction<Commit<?>, Compaction, CompletableFuture<Boolean>> filter = filters.computeIfAbsent(type, t -> {
      for (Map.Entry<Class<? extends Command>, FilterExecutor> entry : filters.entrySet()) {
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
          allOperation = new OperationExecutor(method);
        } else if (!operations.containsKey(operation)) {
          operations.put(operation, new OperationExecutor(method));
        }
      }
    }
  }

  /**
   * Wraps an operation method.
   */
  @SuppressWarnings("unchecked")
  private Function<Commit<?>, CompletableFuture<Object>> wrapOperation(Method method) {
    if (method.getParameterCount() < 1) {
      throw new IllegalStateException("invalid operation method: not enough arguments");
    } else if (method.getParameterCount() > 1) {
      throw new IllegalStateException("invalid operation method: too many arguments");
    } else {
      return commit -> {
        try {
          return (CompletableFuture<Object>) method.invoke(this, commit);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new ApplicationException("failed to invoke operation", e);
        }
      };
    }
  }

  /**
   * Finds the operation method for the given operation.
   */
  private OperationExecutor findOperation(Class<? extends Operation> type) {
    OperationExecutor operation = operations.computeIfAbsent(type, t -> {
      for (Map.Entry<Class<? extends Operation>, OperationExecutor> entry : operations.entrySet()) {
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
   * Filter executor.
   */
  private class FilterExecutor implements BiFunction<Commit<?>, Compaction, CompletableFuture<Boolean>> {
    private final Method method;
    private final boolean async;
    private final boolean singleArg;

    private FilterExecutor(Method method) {
      this.method = method;
      async = method.getReturnType() == CompletableFuture.class;
      singleArg = method.getParameterCount() == 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Boolean> apply(Commit<?> commit, Compaction compaction) {
      if (singleArg) {
        try {
          return async ? (CompletableFuture<Boolean>) method.invoke(StateMachine.this, commit) : CompletableFuture.completedFuture((boolean) method.invoke(StateMachine.this, commit));
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new ApplicationException("failed to invoke operation", e);
        }
      } else {
        try {
          return async ? (CompletableFuture<Boolean>) method.invoke(StateMachine.this, commit, compaction) : CompletableFuture.completedFuture((boolean) method
            .invoke(StateMachine.this, commit, compaction));
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new ApplicationException("failed to invoke operation", e);
        }
      }
    }
  }

  /**
   * Operation executor.
   */
  private class OperationExecutor implements Function<Commit<?>, CompletableFuture<Object>> {
    private final Method method;
    private final boolean async;

    private OperationExecutor(Method method) {
      this.method = method;
      async = method.getReturnType() == CompletableFuture.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Object> apply(Commit<?> commit) {
      try {
        return async ? (CompletableFuture<Object>) method.invoke(StateMachine.this, commit) : CompletableFuture.completedFuture(method.invoke(StateMachine.this, commit));
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new ApplicationException("failed to invoke operation", e);
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
  public CompletableFuture<Boolean> filter(Commit<? extends Command> commit, Compaction compaction) {
    LOGGER.debug("Filtering {}", commit);
    return findFilter(commit.type(), compaction.type()).apply(commit, compaction);
  }

  /**
   * Applies an operation to the state machine.
   *
   * @param commit The commit to apply.
   * @return The operation result.
   */
  public CompletableFuture<Object> apply(Commit<? extends Operation> commit) {
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

  /**
   * Closes the state machine.
   */
  @Override
  public void close() {

  }

}
