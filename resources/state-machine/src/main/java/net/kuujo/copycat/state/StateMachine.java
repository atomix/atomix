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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.resource.PartitionContext;
import net.kuujo.copycat.resource.internal.AbstractPartitionedResource;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * State machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateMachine<T> extends AbstractPartitionedResource<StateMachine<T>, StateMachinePartition<T>> {
  private final StateFactory<T> stateFactory;
  private final InvocationHandler handler = new StateProxyInvocationHandler();

  public StateMachine(StateFactory<T> stateFactory, StateMachineConfig config, ClusterConfig cluster) {
    super(config, cluster);
    if (stateFactory == null)
      throw new NullPointerException("stateFactory cannot be null");
    this.stateFactory = stateFactory;
  }

  public StateMachine(StateFactory<T> stateFactory, StateMachineConfig config, ClusterConfig cluster, Executor executor) {
    super(config, cluster, executor);
    if (stateFactory == null)
      throw new NullPointerException("stateFactory cannot be null");
    this.stateFactory = stateFactory;
  }

  @Override
  protected StateMachinePartition<T> createPartition(PartitionContext context) {
    return new StateMachinePartition<>(stateFactory.createState(), context);
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
   * State proxy invocation handler.
   */
  private class StateProxyInvocationHandler implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Class<?> returnType = method.getReturnType();
      if (returnType == CompletableFuture.class) {
        return partition(args[0]).submit(method, args);
      }
      return partition(args[0]).submit(method, args).get();
    }
  }

}
