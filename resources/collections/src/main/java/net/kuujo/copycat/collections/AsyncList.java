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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.internal.collection.ListState;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Asynchronous list.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The list data type.
 */
public class AsyncList<T> extends AbstractResource<AsyncList<T>> implements AsyncCollection<AsyncList<T>, T>, AsyncListProxy<T> {

  /**
   * Creates a new asynchronous list, loading the log configuration from the classpath.
   *
   * @param <T> The asynchronous list entry type.
   * @return A new asynchronous list instance.
   */
  public static <T> AsyncList<T> create() {
    return create(new AsyncListConfig(), new ClusterConfig());
  }

  /**
   * Creates a new asynchronous list, loading the log configuration from the classpath.
   *
   * @param <T> The asynchronous list entry type.
   * @return A new asynchronous list instance.
   */
  public static <T> AsyncList<T> create(Executor executor) {
    return create(new AsyncListConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new asynchronous list, loading the log configuration from the classpath.
   *
   * @param name The asynchronous list resource name to be used to load the asynchronous list configuration from the classpath.
   * @param <T> The asynchronous list entry type.
   * @return A new asynchronous list instance.
   */
  public static <T> AsyncList<T> create(String name) {
    return create(new AsyncListConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new asynchronous list, loading the log configuration from the classpath.
   *
   * @param name The asynchronous list resource name to be used to load the asynchronous list configuration from the classpath.
   * @param executor An executor on which to execute asynchronous list callbacks.
   * @param <T> The asynchronous list entry type.
   * @return A new asynchronous list instance.
   */
  public static <T> AsyncList<T> create(String name, Executor executor) {
    return create(new AsyncListConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new asynchronous list with the given cluster and asynchronous list configurations.
   *
   * @param name The asynchronous list resource name to be used to load the asynchronous list configuration from the classpath.
   * @param cluster The cluster configuration.
   * @return A new asynchronous list instance.
   */
  public static <T> AsyncList<T> create(String name, ClusterConfig cluster) {
    return create(new AsyncListConfig(name), cluster);
  }

  /**
   * Creates a new asynchronous list with the given cluster and asynchronous list configurations.
   *
   * @param name The asynchronous list resource name to be used to load the asynchronous list configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous list callbacks.
   * @return A new asynchronous list instance.
   */
  public static <T> AsyncList<T> create(String name, ClusterConfig cluster, Executor executor) {
    return create(new AsyncListConfig(name), cluster, executor);
  }

  /**
   * Creates a new asynchronous list with the given cluster and asynchronous list configurations.
   *
   * @param config The asynchronous list configuration.
   * @param cluster The cluster configuration.
   * @return A new asynchronous list instance.
   */
  public static <T> AsyncList<T> create(AsyncListConfig config, ClusterConfig cluster) {
    return new AsyncList<>(config, cluster);
  }

  /**
   * Creates a new asynchronous list with the given cluster and asynchronous list configurations.
   *
   * @param config The asynchronous list configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous list callbacks.
   * @return A new asynchronous list instance.
   */
  public static <T> AsyncList<T> create(AsyncListConfig config, ClusterConfig cluster, Executor executor) {
    return new AsyncList<>(config, cluster, executor);
  }

  private final StateMachine<ListState<T>> stateMachine;
  private AsyncListProxy<T> proxy;

  public AsyncList(AsyncListConfig config, ClusterConfig cluster) {
    this(new ResourceContext(config, cluster));
  }

  public AsyncList(AsyncListConfig config, ClusterConfig cluster, Executor executor) {
    this(new ResourceContext(config, cluster, executor));
  }

  @SuppressWarnings("unchecked")
  public AsyncList(ResourceContext context) {
    super(context);
    this.stateMachine = new StateMachine<>(context);
  }

  /**
   * If the collection is closed, returning a failed CompletableFuture. Otherwise, calls the given supplier to
   * return the completed future result.
   *
   * @param supplier The supplier to call if the collection is open.
   * @param <R> The future result type.
   * @return A completable future that if this collection is closed is immediately failed.
   */
  protected <R> CompletableFuture<R> checkOpen(Supplier<CompletableFuture<R>> supplier) {
    if (proxy == null) {
      return Futures.exceptionalFuture(new IllegalStateException("Collection closed"));
    }
    return supplier.get();
  }

  @Override
  public CompletableFuture<Boolean> add(T value) {
    return checkOpen(() -> proxy.add(value));
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends T> values) {
    return checkOpen(() -> proxy.addAll(values));
  }

  @Override
  public CompletableFuture<T> get(int index) {
    return checkOpen(() -> proxy.get(index));
  }

  @Override
  public CompletableFuture<T> set(int index, T value) {
    return checkOpen(() -> proxy.set(index, value));
  }

  @Override
  public CompletableFuture<Void> add(int index, T value) {
    return checkOpen(() -> proxy.add(index, value));
  }

  @Override
  public CompletableFuture<Boolean> addAll(int index, Collection<? extends T> values) {
    return checkOpen(() -> proxy.addAll(index, values));
  }

  @Override
  public CompletableFuture<T> remove(int index) {
    return checkOpen(() -> proxy.remove(index));
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<?> values) {
    return checkOpen(() -> proxy.retainAll(values));
  }

  @Override
  public CompletableFuture<Boolean> remove(Object value) {
    return checkOpen(() -> proxy.remove(value));
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<?> values) {
    return checkOpen(() -> proxy.removeAll(values));
  }

  @Override
  public CompletableFuture<Boolean> contains(Object value) {
    return checkOpen(() -> proxy.contains(value));
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<?> values) {
    return checkOpen(() -> proxy.containsAll(values));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return checkOpen(proxy::size);
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return checkOpen(proxy::isEmpty);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return checkOpen(proxy::clear);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<AsyncList<T>> open() {
    return stateMachine.open()
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncListProxy.class);
      }).thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}
