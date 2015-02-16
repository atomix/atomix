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

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.util.internal.Assert;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * State machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateMachine<T> extends AbstractResource<StateMachine<T>> {

  /**
   * Creates a new state machine, loading the log configuration from the classpath.
   *
   * @param <T> The state machine entry type.
   * @return A new state machine instance.
   */
  public static <T> StateMachine<T> create() {
    return create(new StateMachineConfig(), new ClusterConfig());
  }

  /**
   * Creates a new state machine, loading the log configuration from the classpath.
   *
   * @param <T> The state machine entry type.
   * @return A new state machine instance.
   */
  public static <T> StateMachine<T> create(Executor executor) {
    return create(new StateMachineConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new state machine, loading the log configuration from the classpath.
   *
   * @param name The state machine resource name to be used to load the state machine configuration from the classpath.
   * @param <T> The state machine entry type.
   * @return A new state machine instance.
   */
  public static <T> StateMachine<T> create(String name) {
    return create(new StateMachineConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new state machine, loading the log configuration from the classpath.
   *
   * @param name The state machine resource name to be used to load the state machine configuration from the classpath.
   * @param executor An executor on which to execute state machine callbacks.
   * @param <T> The state machine entry type.
   * @return A new state machine instance.
   */
  public static <T> StateMachine<T> create(String name, Executor executor) {
    return create(new StateMachineConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new state machine with the given cluster and state machine configurations.
   *
   * @param name The state machine resource name to be used to load the state machine configuration from the classpath.
   * @param cluster The cluster configuration.
   * @return A new state machine instance.
   */
  public static <T> StateMachine<T> create(String name, ClusterConfig cluster) {
    return create(new StateMachineConfig(name), cluster);
  }

  /**
   * Creates a new state machine with the given cluster and state machine configurations.
   *
   * @param name The state machine resource name to be used to load the state machine configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute state machine callbacks.
   * @return A new state machine instance.
   */
  public static <T> StateMachine<T> create(String name, ClusterConfig cluster, Executor executor) {
    return create(new StateMachineConfig(name), cluster, executor);
  }

  /**
   * Creates a new state machine with the given cluster and state machine configurations.
   *
   * @param config The state machine configuration.
   * @param cluster The cluster configuration.
   * @return A new state machine instance.
   */
  public static <T> StateMachine<T> create(StateMachineConfig config, ClusterConfig cluster) {
    return new StateMachine<>(config, cluster);
  }

  /**
   * Creates a new state machine with the given cluster and state machine configurations.
   *
   * @param config The state machine configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute state machine callbacks.
   * @return A new state machine instance.
   */
  public static <T> StateMachine<T> create(StateMachineConfig config, ClusterConfig cluster, Executor executor) {
    return new StateMachine<>(config, cluster, executor);
  }

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
      return StateMachine.this.cluster();
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
      StateMachine.this.state = state;
      initialize();
      return this;
    }
  };

  public StateMachine(StateMachineConfig config, ClusterConfig cluster) {
    this(new ResourceContext(config, cluster));
  }

  public StateMachine(StateMachineConfig config, ClusterConfig cluster, Executor executor) {
    this(new ResourceContext(config, cluster, executor));
  }

  @SuppressWarnings("unchecked")
  public StateMachine(ResourceContext context) {
    super(context);
    this.stateType = Assert.isNotNull(context.<StateMachineConfig>config().getStateType(), "stateType");
    try {
      this.state = (T) Assert.isNotNull(context.<StateMachineConfig>config().getInitialState(), "initialState").newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
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

  /**
   * Takes a snapshot of the status machine status.
   */
  private Map<String, Object> snapshot() {
    Map<String, Object> snapshot = new HashMap<>(2);
    snapshot.put("state", state.getClass().getName());
    snapshot.put("data", data);
    return snapshot;
  }

  /**
   * Installs a snapshot of the status machine status.
   */
  @SuppressWarnings("unchecked")
  private void install(Map<String, Object> snapshot) {
    Object stateClassName = snapshot.get("state");
    if (stateClassName == null) {
      throw new IllegalStateException("Invalid snapshot");
    }
    try {
      Class<?> stateClass = Class.forName(stateClassName.toString());
      this.state = (T) stateClass.newInstance();
      initialize();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new IllegalStateException("Invalid snapshot state");
    }
    this.data = (Map<String, Object>) snapshot.get("data");
  }

  @Override
  public synchronized CompletableFuture<StateMachine<T>> open() {
    log.snapshotWith(this::snapshot);
    log.installWith(this::install);
    return log.open().thenApply(v -> this);
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
    });
  }

  @Override
  public boolean isClosed() {
    return log.isClosed();
  }

  /**
   * Registers commands on the status log.
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
   * Initializes the current status by locating the @Initializer method on the status class and caching the method.
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
        .append(String.join(",", Arrays.asList(m.getParameterTypes()).stream().map(Class::getCanonicalName).collect(Collectors
          .toList())))
        .append(')')
        .toString();
    });
  }

  /**
   * Wraps a status log operation for the given method.
   *
   * @param method The method for which to create the status log command.
   * @return The generated status log command.
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
