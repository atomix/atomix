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

import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.internal.collection.AsyncListState;
import net.kuujo.copycat.collections.internal.collection.DefaultAsyncList;
import net.kuujo.copycat.collections.internal.collection.DefaultAsyncListState;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.log.LogConfig;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Asynchronous list.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The list data type.
 */
public interface AsyncList<T> extends AsyncCollection<T> {

  /**
   * Creates a new asynchronous list.
   *
   * @param name The asynchronous list name.
   * @param uri The asynchronous list member URI.
   * @param cluster The cluster configuration.
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  static <T> AsyncList<T> create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, new LogConfig(), Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-list-" + name + "-%d")));
  }

  /**
   * Creates a new asynchronous list.
   *
   * @param name The asynchronous list name.
   * @param uri The asynchronous list member URI.
   * @param cluster The cluster configuration.
   * @param config The list configuration.   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  static <T> AsyncList<T> create(String name, String uri, ClusterConfig cluster, LogConfig config) {
    return create(name, uri, cluster, config, Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-list-" + name + "-%d")));
  }

  /**
   * Creates a new asynchronous list.
   *
   * @param name The asynchronous list name.
   * @param uri The asynchronous list member URI.
   * @param cluster The cluster configuration.
   * @param executor The user execution context.
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  static <T> AsyncList<T> create(String name, String uri, ClusterConfig cluster, Executor executor) {
    return create(name, uri, cluster, new LogConfig(), executor);
  }

  /**
   * Creates a new asynchronous list.
   *
   * @param name The asynchronous list name.
   * @param uri The asynchronous list member URI.
   * @param cluster The cluster configuration.
   * @param config The list configuration.
   * @param executor The user execution context.
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  @SuppressWarnings("unchecked")
  static <T> AsyncList<T> create(String name, String uri, ClusterConfig cluster, LogConfig config, Executor executor) {
    return new DefaultAsyncList(StateMachine.create(name, uri, AsyncListState.class, new DefaultAsyncListState<>(), cluster, config, executor));
  }

  /**
   * Gets a entry at a specific index in the list.
   *
   * @param index The index of the entry to get.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<T> get(int index);

  /**
   * Sets an index in the list.
   *
   * @param index The index to set.
   * @param value The entry to set.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Void> set(int index, T value);

  /**
   * Removes an index in the list.
   *
   * @param index The index to remove.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<T> remove(int index);

}
