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
import net.kuujo.copycat.collections.internal.collection.DefaultSetState;
import net.kuujo.copycat.collections.internal.collection.SetState;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Asynchronous set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The set data type.
 */
public class AsyncSet<T> extends AbstractResource<AsyncSet<T>> implements AsyncCollection<AsyncSet<T>, T>, AsyncSetProxy<T> {

  /**
   * Creates a new asynchronous set with the given cluster.
   *
   * @param cluster The cluster configuration.
   * @return A new asynchronous set instance.
   */
  public static <T> AsyncSet<T> create(ClusterConfig cluster) {
    return new AsyncSet<>(new AsyncSetConfig(), cluster);
  }

  /**
   * Creates a new asynchronous set with the given cluster.
   *
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous set callbacks.
   * @return A new asynchronous set instance.
   */
  public static <T> AsyncSet<T> create(ClusterConfig cluster, Executor executor) {
    return new AsyncSet<>(new AsyncSetConfig(), cluster, executor);
  }

  /**
   * Creates a new asynchronous set with the given cluster and asynchronous set configurations.
   *
   * @param config The asynchronous set configuration.
   * @param cluster The cluster configuration.
   * @return A new asynchronous set instance.
   */
  public static <T> AsyncSet<T> create(AsyncSetConfig config, ClusterConfig cluster) {
    return new AsyncSet<>(config, cluster);
  }

  /**
   * Creates a new asynchronous set with the given cluster and asynchronous set configurations.
   *
   * @param config The asynchronous set configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute asynchronous set callbacks.
   * @return A new asynchronous set instance.
   */
  public static <T> AsyncSet<T> create(AsyncSetConfig config, ClusterConfig cluster, Executor executor) {
    return new AsyncSet<>(config, cluster, executor);
  }

  private final StateMachine<SetState<T>> stateMachine;
  private AsyncSetProxy<T> proxy;

  public AsyncSet(AsyncSetConfig config, ClusterConfig cluster) {
    this(new ResourceContext(config, cluster));
  }

  public AsyncSet(AsyncSetConfig config, ClusterConfig cluster, Executor executor) {
    this(new ResourceContext(config, cluster, executor));
  }

  @SuppressWarnings("unchecked")
  public AsyncSet(ResourceContext context) {
    super(context);
    this.stateMachine = new StateMachine<>(new DefaultSetState<>(), context);
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
  public synchronized CompletableFuture<AsyncSet<T>> open() {
    return stateMachine.open()
      .thenRun(() -> {
        this.proxy = stateMachine.createProxy(AsyncSetProxy.class);
      }).thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}
