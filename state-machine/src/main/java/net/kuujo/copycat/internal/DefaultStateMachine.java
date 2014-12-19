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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.*;
import net.kuujo.copycat.cluster.Cluster;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Default state machine implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultStateMachine<T> implements StateMachine<T> {
  private final Class<T> stateType;
  private T state;
  private final StateLog<List<Object>> log;
  private final InvocationHandler handler = new StateProxyInvocationHandler();
  private Map<String, Object> data = new HashMap<>(1024);
  private final StateContext<T> context = new StateContext<T>() {
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
      return this;
    }
  };

  public DefaultStateMachine(Class<T> stateType, T state, StateLog<List<Object>> log) {
    if (!stateType.isInterface()) {
      throw new IllegalArgumentException("State type must be an interface");
    }
    this.stateType = stateType;
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

  @Override
  public CopycatState state() {
    return log.state();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <U> U createProxy(Class<U> type) {
    return (U) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{type}, handler);
  }

  @Override
  public <U> CompletableFuture<U> submit(String command, Object... args) {
    return log.submit(command, new ArrayList<>(Arrays.asList(args)));
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
  public CompletableFuture<Void> open() {
    log.snapshotter(this::snapshot);
    log.installer(this::install);
    return log.open();
  }

  @Override
  public CompletableFuture<Void> close() {
    return log.close().whenComplete((result, error) -> {
      log.snapshotter(null);
      log.installer(null);
    });
  }

  @Override
  public CompletableFuture<Void> delete() {
    return log.delete();
  }

  /**
   * Registers commands on the state log.
   */
  private void registerCommands() {
    for (Method method : stateType.getMethods()) {
      CommandInfo info = method.getAnnotation(CommandInfo.class);
      if (info == null) {
        log.register(method.getName(), createCommand(method));
      } else {
        log.register(info.name().equals("") ? method.getName() : info.name(), createCommand(method), new CommandOptions().withConsistent(info.consistent()).withReadOnly(info.readOnly()));
      }
    }
  }

  /**
   * Creates a state log command for the given method.
   *
   * @param method The method for which to create the state log command.
   * @return The generated state log command.
   */
  private Command<List<Object>, Object> createCommand(Method method) {
    Integer tempIndex = null;
    Class<?>[] paramTypes = method.getParameterTypes();
    for (int i = 0; i < paramTypes.length; i++) {
      if (StateContext.class.isAssignableFrom(paramTypes[i])) {
        tempIndex = i;
      }
    }
    final Integer contextIndex = tempIndex;

    return values -> {
      Object[] emptyArgs = new Object[values.size() + (contextIndex != null ? 1 : 0)];
      Object[] args = values.toArray(emptyArgs);
      if (contextIndex != null) {
        Object lastArg = null;
        for (int i = 0; i < args.length; i++) {
          if (i > contextIndex) {
            args[i] = lastArg;
            lastArg = args[i];
          } else if (i == contextIndex) {
            lastArg = args[i];
            args[i] = context;
          }
        }
      }

      Object[] compiledArgs;
      if (contextIndex != null) {
        compiledArgs = values.toArray(new Object[values.size() + 1]);
        compiledArgs[compiledArgs.length-1] = context;
      } else {
        compiledArgs = values.toArray(new Object[values.size()]);
      }

      try {
        return method.invoke(state, compiledArgs);
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
        return submit(method.getName(), args);
      }
      return submit(method.getName(), args).get();
    }
  }

}
