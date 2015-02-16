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

import net.kuujo.copycat.atomic.internal.ReferenceState;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.state.StateMachine;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Asynchronous atomic value.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncReference<T> extends AbstractResource<AsyncReference<T>> implements AsyncReferenceProxy<T> {

  /**
   * Creates a new asynchronous atomic reference, loading the log configuration from the classpath.
   *
   * @param <T> The asynchronous atomic reference entry type.
   * @return A new asynchronous atomic reference instance.
   */
  public static <T> AsyncReference<T> create() {
    return create(new AsyncReferenceConfig(), new ClusterConfig());
  }

  /**
   * Creates a new asynchronous atomic reference, loading the log configuration from the classpath.
   *
   * @param <T> The asynchronous atomic reference entry type.
   * @return A new asynchronous atomic reference instance.
   */
  public static <T> AsyncReference<T> create(Executor executor) {
    return create(new AsyncReferenceConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new asynchronous atomic reference, loading the log configuration from the classpath.
   *
   * @param name The asynchronous atomic reference resource name to be used to load the asynchronous atomic reference configuration from the classpath.
   * @param <T> The asynchronous atomic reference entry type.
   * @return A new asynchronous atomic reference instance.
   */
  public static <T> AsyncReference<T> create(String name) {
    return create(new AsyncReferenceConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new asynchronous atomic reference, loading the log configuration from the classpath.
   *
   * @param name The asynchronous atomic reference resource name to be used to load the asynchronous atomic reference configuration from the classpath.
   * @param executor An executor on which to execute asynchronous atomic reference callbacks.
   * @param <T> The asynchronous atomic reference entry type.
   * @return A new asynchronous atomic reference instance.
   */
  public static <T> AsyncReference<T> create(String name, Executor executor) {
    return create(new AsyncReferenceConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new asynchronous atomic reference with the given cluster and asynchronous atomic reference configurations.
   *
   * @param name The asynchronous atomic reference resource name to be used to load the asynchronous atomic reference configuration from the classpath.
   * @param cluster The cluster configuration.
   * @return A new asynchronous atomic reference instance.
   */
  public static <T> AsyncReference<T> create(String name, ClusterConfig cluster) {
    return create(new AsyncReferenceConfig(name), cluster);
  }

  /**
   * Creates a new asynchronous atomic reference with the given cluster and asynchronous atomic reference configurations.
   *
   * @param name The asynchronous atomic reference resource name to be used to load the asynchronous atomic reference configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous atomic reference callbacks.
   * @return A new asynchronous atomic reference instance.
   */
  public static <T> AsyncReference<T> create(String name, ClusterConfig cluster, Executor executor) {
    return create(new AsyncReferenceConfig(name), cluster, executor);
  }

  /**
   * Creates a new asynchronous atomic reference with the given cluster and asynchronous atomic reference configurations.
   *
   * @param config The asynchronous atomic reference configuration.
   * @param cluster The cluster configuration.
   * @return A new asynchronous atomic reference instance.
   */
  public static <T> AsyncReference<T> create(AsyncReferenceConfig config, ClusterConfig cluster) {
    return new AsyncReference<>(config, cluster);
  }

  /**
   * Creates a new asynchronous atomic reference with the given cluster and asynchronous atomic reference configurations.
   *
   * @param config The asynchronous atomic reference configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous atomic reference callbacks.
   * @return A new asynchronous atomic reference instance.
   */
  public static <T> AsyncReference<T> create(AsyncReferenceConfig config, ClusterConfig cluster, Executor executor) {
    return new AsyncReference<>(config, cluster, executor);
  }

  private StateMachine<ReferenceState<T>> stateMachine;
  private AsyncReferenceProxy<T> proxy;

  public AsyncReference(AsyncReferenceConfig config, ClusterConfig cluster) {
    this(new ResourceContext(config, cluster));
  }

  public AsyncReference(AsyncReferenceConfig config, ClusterConfig cluster, Executor executor) {
    this(new ResourceContext(config, cluster, executor));
  }

  @SuppressWarnings("unchecked")
  public AsyncReference(ResourceContext context) {
    super(context);
    this.stateMachine = new StateMachine<>(context);
  }

  @Override
  public CompletableFuture<T> get() {
    return proxy.get();
  }

  @Override
  public CompletableFuture<Void> set(T value) {
    return proxy.set(value);
  }

  @Override
  public CompletableFuture<T> getAndSet(T value) {
    return proxy.getAndSet(value);
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(T expect, T update) {
    return proxy.compareAndSet(expect, update);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<AsyncReference<T>> open() {
    return stateMachine.open()
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncReferenceProxy.class);
      }).thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}
