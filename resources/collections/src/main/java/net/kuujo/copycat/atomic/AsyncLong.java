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

import net.kuujo.copycat.atomic.internal.LongState;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.state.StateMachine;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Asynchronous atomic long.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncLong extends AbstractResource<AsyncLong> implements AsyncLongProxy {

  /**
   * Creates a new asynchronous atomic long, loading the log configuration from the classpath.
   *
   * @return A new asynchronous atomic long instance.
   */
  public static AsyncLong create() {
    return create(new AsyncLongConfig(), new ClusterConfig());
  }

  /**
   * Creates a new asynchronous atomic long, loading the log configuration from the classpath.
   *
   * @return A new asynchronous atomic long instance.
   */
  public static AsyncLong create(Executor executor) {
    return create(new AsyncLongConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new asynchronous atomic long, loading the log configuration from the classpath.
   *
   * @param name The asynchronous atomic long resource name to be used to load the asynchronous atomic long configuration from the classpath.
   * @return A new asynchronous atomic long instance.
   */
  public static AsyncLong create(String name) {
    return create(new AsyncLongConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new asynchronous atomic long, loading the log configuration from the classpath.
   *
   * @param name The asynchronous atomic long resource name to be used to load the asynchronous atomic long configuration from the classpath.
   * @param executor An executor on which to execute asynchronous atomic long callbacks.
   * @return A new asynchronous atomic long instance.
   */
  public static AsyncLong create(String name, Executor executor) {
    return create(new AsyncLongConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new asynchronous atomic long with the given cluster and asynchronous atomic long configurations.
   *
   * @param name The asynchronous atomic long resource name to be used to load the asynchronous atomic long configuration from the classpath.
   * @param cluster The cluster configuration.
   * @return A new asynchronous atomic long instance.
   */
  public static AsyncLong create(String name, ClusterConfig cluster) {
    return create(new AsyncLongConfig(name), cluster);
  }

  /**
   * Creates a new asynchronous atomic long with the given cluster and asynchronous atomic long configurations.
   *
   * @param name The asynchronous atomic long resource name to be used to load the asynchronous atomic long configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous atomic long callbacks.
   * @return A new asynchronous atomic long instance.
   */
  public static AsyncLong create(String name, ClusterConfig cluster, Executor executor) {
    return create(new AsyncLongConfig(name), cluster, executor);
  }

  /**
   * Creates a new asynchronous atomic long with the given cluster and asynchronous atomic long configurations.
   *
   * @param config The asynchronous atomic long configuration.
   * @param cluster The cluster configuration.
   * @return A new asynchronous atomic long instance.
   */
  public static AsyncLong create(AsyncLongConfig config, ClusterConfig cluster) {
    return new AsyncLong(config, cluster);
  }

  /**
   * Creates a new asynchronous atomic long with the given cluster and asynchronous atomic long configurations.
   *
   * @param config The asynchronous atomic long configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous atomic long callbacks.
   * @return A new asynchronous atomic long instance.
   */
  public static AsyncLong create(AsyncLongConfig config, ClusterConfig cluster, Executor executor) {
    return new AsyncLong(config, cluster, executor);
  }

  private StateMachine<LongState> stateMachine;
  private AsyncLongProxy proxy;

  public AsyncLong(AsyncLongConfig config, ClusterConfig cluster) {
    this(new ResourceContext(config, cluster));
  }

  public AsyncLong(AsyncLongConfig config, ClusterConfig cluster, Executor executor) {
    this(new ResourceContext(config, cluster, executor));
  }

  public AsyncLong(ResourceContext context) {
    super(context);
    this.stateMachine = new StateMachine<>(context);
  }

  @Override
  public CompletableFuture<Long> get() {
    return proxy.get();
  }

  @Override
  public CompletableFuture<Void> set(long value) {
    return proxy.set(value);
  }

  @Override
  public CompletableFuture<Long> addAndGet(long value) {
    return proxy.addAndGet(value);
  }

  @Override
  public CompletableFuture<Long> getAndAdd(long value) {
    return proxy.getAndAdd(value);
  }

  @Override
  public CompletableFuture<Long> getAndSet(long value) {
    return proxy.getAndSet(value);
  }

  @Override
  public CompletableFuture<Long> getAndIncrement() {
    return proxy.getAndIncrement();
  }

  @Override
  public CompletableFuture<Long> getAndDecrement() {
    return proxy.getAndDecrement();
  }

  @Override
  public CompletableFuture<Long> incrementAndGet() {
    return proxy.incrementAndGet();
  }

  @Override
  public CompletableFuture<Long> decrementAndGet() {
    return proxy.decrementAndGet();
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(long expect, long update) {
    return proxy.compareAndSet(expect, update);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<AsyncLong> open() {
    return stateMachine.open()
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncLongProxy.class);
      }).thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}
