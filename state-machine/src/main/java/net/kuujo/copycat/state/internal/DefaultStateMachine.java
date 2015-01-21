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
package net.kuujo.copycat.state.internal;

import net.kuujo.copycat.*;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.internal.AbstractResource;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.state.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Default state machine implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultStateMachine<T> extends AbstractResource<StateMachine<T>> implements StateMachine<T> {
  private final Class<T> stateType;
  private T state;
  private final StateLog<List<Object>> log;
  private final InvocationHandler handler = new StateProxyInvocationHandler();
  private Map<String, Object> data = new HashMap<>(1024);
  private final Map<Class<?>, Method> initializers = new HashMap<>();
  private final Map<Method, String> methodCache = new ConcurrentHashMap<>();
  private final StateContext<T> context = new StateContext<T>() {
    @Override
    public Cluster cluster() {
      return DefaultStateMachine.this.cluster();
    }

    @Override
    public T state() {
      return state;
    }

    @Override
    public StateContext<T> put(String key, Object value) {
      data.put(key, value);
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <U> U get(String key) {
      return (U) data.get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <U> U remove(String key) {
      return (U) data.remove(key);
    }

    @Override
    public StateContext<T> clear() {
      data.clear();
      return this;
    }

    @Override
    public StateContext<T> transition(T state) {
      DefaultStateMachine.this.state = state;
      initialize();
      return this;
    }
  };

  public DefaultStateMachine(ResourceContext context, Class<T> stateType, Class<? extends T> initialState) {
    super(context);
    this.stateType = Assert.isNotNull(stateType, "stateType");
    try {
      this.state = Assert.isNotNull(initialState, "initialState").newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    this.log = new DefaultStateLog<>(context);
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

  @Override
  public CopycatState state() {
    return log.state();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <U> U createProxy(Class<U> type) {
    return (U) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{type}, handler);
  }

  /**
   * Takes a snapshot of the state machine state.
   */
  private Map<String, Object> snapshot() {
    return data;
  }

  /**
   * Installs a snapshot of the state machine state.
   */
  private void install(Map<String, Object> snapshot) {
    this.data = snapshot;
  }

  @Override
  public synchronized CompletableFuture<StateMachine<T>> open() {
    log.snapshotWith(this::snapshot);
    log.installWith(this::install);
    return runStartupTasks()
      .thenComposeAsync(v -> log.open(), executor)
      .thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return log.isOpen();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return log.close().whenComplete((result, error) -> {
      log.snapshotWith(null);
      log.installWith(null);
    }).thenComposeAsync(v -> runShutdownTasks(), executor);
  }

  @Override
  public boolean isClosed() {
    return log.isClosed();
  }

  /**
   * Registers commands on the state log.
   */
  private void registerCommands() {
    for (Method method : stateType.getMethods()) {
      Query query = method.getAnnotation(Query.class);
      if (query != null) {
        log.registerQuery(getOperationName(method), wrapOperation(method), query.consistency());
      } else {
        Command command = method.getAnnotation(Command.class);
        if (command != null || Modifier.isPublic(method.getModifiers())) {
          log.registerCommand(getOperationName(method), wrapOperation(method));
        }
      }
    }
    initialize();
  }

  /**
   * Initializes the current state by locating the @Initializer method on the state class and caching the method.
   */
  private void initialize() {
    Method initializer = initializers.get(state.getClass());
    if (initializer == null) {
      for (Method method : state.getClass().getMethods()) {
        if (method.isAnnotationPresent(Initializer.class)) {
          initializer = method;
          break;
        }
      }
      if (initializer != null) {
        initializers.put(state.getClass(), initializer);
      }
    }
    if (initializer != null) {
      try {
        initializer.invoke(state, context);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /**
   * Gets the cached method operation name or generates and caches an operation name if it's not already cached.
   */
  private String getOperationName(Method method) {
    return methodCache.computeIfAbsent(method, m -> {
      return new StringBuilder()
        .append(m.getName())
        .append('(')
        .append(String.join(",", Arrays.asList(m.getParameterTypes()).stream().map(Class::getCanonicalName).collect(Collectors.toList())))
        .append(')')
        .toString();
    });
  }

  /**
   * Wraps a state log operation for the given method.
   *
   * @param method The method for which to create the state log command.
   * @return The generated state log command.
   */
  private Function<List<Object>, Object> wrapOperation(Method method) {
    return values -> {
      try {
        return method.invoke(state, values.toArray(new Object[values.size()]));
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
