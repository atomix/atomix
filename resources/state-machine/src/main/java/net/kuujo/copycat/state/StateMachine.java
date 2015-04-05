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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * State machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateMachine<T> extends AbstractResource<StateMachine<T>> {

  /**
   * Creates a new state machine with the given cluster and state machine configurations.
   *
   * @param config The state machine configuration.
   * @param cluster The cluster configuration.
   * @return A new state machine instance.
   */
  public static <T> StateMachine<T> create(T state, StateMachineConfig config, ClusterConfig cluster) {
    return new StateMachine<>(state, config, cluster);
  }

  /**
   * Creates a new state machine with the given cluster and state machine configurations.
   *
   * @param config The state machine configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute state machine callbacks.
   * @return A new state machine instance.
   */
  public static <T> StateMachine<T> create(T state, StateMachineConfig config, ClusterConfig cluster, Executor executor) {
    return new StateMachine<>(state, config, cluster, executor);
  }

  private T state;
  private final StateLog<Object, List<Object>> log;
  private final InvocationHandler handler = new StateProxyInvocationHandler();
  private final Map<Method, String> methodCache = new ConcurrentHashMap<>();

  public StateMachine(T state, StateMachineConfig config, ClusterConfig cluster) {
    this(state, new ResourceContext(config, cluster));
  }

  public StateMachine(T state, StateMachineConfig config, ClusterConfig cluster, Executor executor) {
    this(state, new ResourceContext(config, cluster, executor));
  }

  @SuppressWarnings("unchecked")
  public StateMachine(T state, ResourceContext context) {
    super(context);
    if (state == null)
      throw new NullPointerException("state cannot be null");
    this.state = state;
    this.log = new StateLog<>(context);
    registerCommands();
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

  @Override
  public synchronized CompletableFuture<StateMachine<T>> open() {
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
    return methodCache.computeIfAbsent(method, m -> new StringBuilder()
      .append(m.getName())
      .append('(')
      .append(String.join(",", Arrays.asList(m.getParameterTypes()).stream().map(Class::getCanonicalName).collect(Collectors.toList())))
      .append(')')
      .toString());
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
   * State proxy invocation handler.
   */
  private class StateProxyInvocationHandler implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Class<?> returnType = method.getReturnType();
      if (returnType == CompletableFuture.class) {
        return log.submit(getOperationName(method), new ArrayList<>(Arrays.asList(args != null ? args : new Object[0])));
      }
      return log.submit(getOperationName(method), new ArrayList<>(Arrays.asList(args != null ? args : new Object[0]))).get();
    }
  }

}
