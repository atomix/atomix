/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.state;

import net.kuujo.copycat.resource.PartitionContext;
import net.kuujo.copycat.resource.internal.AbstractPartition;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * State machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateMachinePartition<T> extends AbstractPartition<StateMachinePartition<T>> {
  private T state;
  private final StateLogPartition<Object, List<Object>> log;
  private final Map<Method, String> methodCache = new ConcurrentHashMap<>();

  public StateMachinePartition(T state, PartitionContext context) {
    super(context);
    if (state == null)
      throw new NullPointerException("state cannot be null");
    this.state = state;
    this.log = new StateLogPartition<>(context);
    registerCommands();
  }

  @Override
  public synchronized CompletableFuture<StateMachinePartition<T>> open() {
    return log.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return log.isOpen();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return log.close();
  }

  @Override
  public boolean isClosed() {
    return log.isClosed();
  }

  /**
   * Registers commands on the status log.
   */
  private void registerCommands() {
    for (Method method : state.getClass().getMethods()) {
      Read query = method.getAnnotation(Read.class);
      if (query != null) {
        log.register(getOperationName(method), Command.Type.READ, wrapOperation(method), query.consistency());
      } else {
        Delete delete = method.getAnnotation(Delete.class);
        if (delete != null) {
          log.register(getOperationName(method), Command.Type.DELETE, wrapOperation(method));
        } else {
          Write command = method.getAnnotation(Write.class);
          if (command != null || Modifier.isPublic(method.getModifiers())) {
            log.register(getOperationName(method), Command.Type.WRITE, wrapOperation(method));
          }
        }
      }
    }
  }

  /**
   * Gets the cached method operation name or generates and caches an operation name if it's not already cached.
   */
  private String getOperationName(Method method) {
    return methodCache.computeIfAbsent(method, m -> {
      return m.getName() + "(" + String.join(",", Arrays.asList(m.getParameterTypes()).stream().map(Class::getCanonicalName).collect(Collectors.toList())) + ")";
    });
  }

  /**
   * Wraps a status log operation for the given method.
   *
   * @param method The method for which to create the status log command.
   * @return The generated status log command.
   */
  private Command<Object, List<Object>, Object> wrapOperation(Method method) {
    return (key, values) -> {
      try {
        Object[] args = new Object[values.size() + 1];
        args[0] = key;
        for (int i = 0; i < values.size(); i++) {
          args[i+1] = values.get(i);
        }
        return method.invoke(state, args);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    };
  }

  /**
   * Submits a command to the state machine partition.
   */
  protected <U> CompletableFuture<U> submit(Method method, Object[] args) {
    return log.submit(getOperationName(method), Arrays.asList(args));
  }

}
