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
package net.kuujo.copycat.state;

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.protocol.Persistence;
import net.kuujo.copycat.resource.Resource;

import java.lang.reflect.*;
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
public class StateMachine<T> implements Resource<StateMachine<T>> {

  /**
   * Returns a new state log builder.
   *
   * @param <T> The state type.
   * @return The state log builder.
   */
  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  /**
   * State machine builder.
   */
  public static class Builder<T> implements net.kuujo.copycat.Builder<StateMachine<T>> {
    private T state;
    private StateLog stateLog;

    private Builder() {
    }

    /**
     * Sets the state machine state class.
     *
     * @param state The state machine state class.
     * @return The state machine builder.
     */
    public Builder<T> withState(T state) {
      this.state = state;
      return this;
    }

    /**
     * Sets the state machine state log.
     *
     * @param stateLog The state machine state log.
     * @return The state machine builder.
     */
    public Builder<T> withLog(StateLog stateLog) {
      this.stateLog = stateLog;
      return this;
    }

    @Override
    public StateMachine<T> build() {
      if (state == null)
        throw new ConfigurationException("state cannot be null");
      if (stateLog == null)
        throw new ConfigurationException("log cannot be null");
      return new StateMachine<>(state, stateLog);
    }
  }

  private final T state;
  private final StateLog log;
  private final InvocationHandler handler = new StateProxyInvocationHandler();
  private final Map<Method, String> methodCache = new ConcurrentHashMap<>();

  public StateMachine(T state, StateLog log) {
    this.state = state;
    this.log = log;
    registerCommands();
  }

  @Override
  public String name() {
    return log.name();
  }

  @Override
  public Cluster cluster() {
    return log.cluster();
  }

  /**
   * Creates a status machine proxy.
   *
   * @param type The proxy interface.
   * @param <U> The proxy type.
   * @return The proxy object.
   */
  @SuppressWarnings("unchecked")
  public <U> U createProxy(Class<U> type) {
    return (U) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{type}, handler);
  }

  /**
   * Submits a command to the state machine.
   */
  @SuppressWarnings("unchecked")
  protected <U> CompletableFuture<U> submit(Method method, Object[] args) {
    return log.submit(getCommandName(method), args[0], Arrays.asList(args));
  }

  /**
   * Registers commands on the status log.
   */
  private void registerCommands() {
    for (Method method : state.getClass().getMethods()) {
      Read read = method.getAnnotation(Read.class);
      if (read != null) {
        registerCommand(getCommandName(method), Command.Type.READ, wrapCommand(method), read.persistence(), read.consistency());
      } else {
        Delete delete = method.getAnnotation(Delete.class);
        if (delete != null) {
          registerCommand(getCommandName(method), Command.Type.DELETE, wrapCommand(method), delete.persistence(), delete.consistency());
        } else {
          Write write = method.getAnnotation(Write.class);
          if (write != null) {
            registerCommand(getCommandName(method), Command.Type.WRITE, wrapCommand(method), write.persistence(), write.consistency());
          } else if (Modifier.isPublic(method.getModifiers())) {
            registerCommand(getCommandName(method), Command.Type.WRITE, wrapCommand(method), Persistence.PERSISTENT, Consistency.STRICT);
          }
        }
      }
    }
  }

  /**
   * Registers a state log command.
   */
  @SuppressWarnings("unchecked")
  private void registerCommand(String name, Command.Type type, Command command, Persistence persistence, Consistency consistency) {
    if (log instanceof DiscreteStateLog) {
      ((DiscreteStateLog) log).register(name, type, command, persistence, consistency);
    } else if (log instanceof PartitionedStateLog) {
      ((PartitionedStateLog) log).register(name, type, command, persistence, consistency);
    }
  }

  /**
   * Gets the cached method operation name or generates and caches an operation name if it's not already cached.
   */
  private String getCommandName(Method method) {
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
  private Command<Object, List<Object>, Object> wrapCommand(Method method) {
    return (key, values) -> {
      try {
        return method.invoke(state, values.toArray());
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<StateMachine<T>> open() {
    return log.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return log.isOpen();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    return log.close();
  }

  @Override
  public boolean isClosed() {
    return log.isClosed();
  }

  /**
   * State proxy invocation handler.
   */
  private class StateProxyInvocationHandler implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Class<?> returnType = method.getReturnType();
      if (returnType == CompletableFuture.class) {
        return submit(method, args);
      }
      return submit(method, args).get();
    }
  }

}
