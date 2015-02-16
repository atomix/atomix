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
package net.kuujo.copycat.atomic;

import net.kuujo.copycat.atomic.internal.BooleanState;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.state.StateMachine;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Asynchronous atomic boolean.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncBoolean extends AbstractResource<AsyncBoolean> implements AsyncBooleanProxy {

  /**
   * Creates a new asynchronous boolean, loading the log configuration from the classpath.
   *
   * @return A new asynchronous boolean instance.
   */
  public static AsyncBoolean create() {
    return create(new AsyncBooleanConfig(), new ClusterConfig());
  }

  /**
   * Creates a new asynchronous boolean, loading the log configuration from the classpath.
   *
   * @return A new asynchronous boolean instance.
   */
  public static AsyncBoolean create(Executor executor) {
    return create(new AsyncBooleanConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new asynchronous boolean, loading the log configuration from the classpath.
   *
   * @param name The asynchronous boolean resource name to be used to load the asynchronous boolean configuration from the classpath.
   * @return A new asynchronous boolean instance.
   */
  public static AsyncBoolean create(String name) {
    return create(new AsyncBooleanConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new asynchronous boolean, loading the log configuration from the classpath.
   *
   * @param name The asynchronous boolean resource name to be used to load the asynchronous boolean configuration from the classpath.
   * @param executor An executor on which to execute asynchronous boolean callbacks.
   * @return A new asynchronous boolean instance.
   */
  public static AsyncBoolean create(String name, Executor executor) {
    return create(new AsyncBooleanConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new asynchronous boolean with the given cluster and asynchronous boolean configurations.
   *
   * @param name The asynchronous boolean resource name to be used to load the asynchronous boolean configuration from the classpath.
   * @param cluster The cluster configuration.
   * @return A new asynchronous boolean instance.
   */
  public static AsyncBoolean create(String name, ClusterConfig cluster) {
    return create(new AsyncBooleanConfig(name), cluster);
  }

  /**
   * Creates a new asynchronous boolean with the given cluster and asynchronous boolean configurations.
   *
   * @param name The asynchronous boolean resource name to be used to load the asynchronous boolean configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous boolean callbacks.
   * @return A new asynchronous boolean instance.
   */
  public static AsyncBoolean create(String name, ClusterConfig cluster, Executor executor) {
    return create(new AsyncBooleanConfig(name), cluster, executor);
  }

  /**
   * Creates a new asynchronous boolean with the given cluster and asynchronous boolean configurations.
   *
   * @param config The asynchronous boolean configuration.
   * @param cluster The cluster configuration.
   * @return A new asynchronous boolean instance.
   */
  public static AsyncBoolean create(AsyncBooleanConfig config, ClusterConfig cluster) {
    return new AsyncBoolean(config, cluster);
  }

  /**
   * Creates a new asynchronous boolean with the given cluster and asynchronous boolean configurations.
   *
   * @param config The asynchronous boolean configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous boolean callbacks.
   * @return A new asynchronous boolean instance.
   */
  public static AsyncBoolean create(AsyncBooleanConfig config, ClusterConfig cluster, Executor executor) {
    return new AsyncBoolean(config, cluster, executor);
  }

  private StateMachine<BooleanState> stateMachine;
  private AsyncBooleanProxy proxy;

  public AsyncBoolean(AsyncBooleanConfig config, ClusterConfig cluster) {
    this(new ResourceContext(config, cluster));
  }

  public AsyncBoolean(AsyncBooleanConfig config, ClusterConfig cluster, Executor executor) {
    this(new ResourceContext(config, cluster, executor));
  }

  public AsyncBoolean(ResourceContext context) {
    super(context);
    this.stateMachine = new StateMachine<>(context);
  }

  @Override
  public CompletableFuture<Boolean> get() {
    return proxy.get();
  }

  @Override
  public CompletableFuture<Void> set(boolean value) {
    return proxy.set(value);
  }

  @Override
  public CompletableFuture<Boolean> getAndSet(boolean value) {
    return proxy.getAndSet(value);
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(boolean expect, boolean update) {
    return proxy.compareAndSet(expect, update);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<AsyncBoolean> open() {
    return stateMachine.open()
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncBooleanProxy.class);
      }).thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}
