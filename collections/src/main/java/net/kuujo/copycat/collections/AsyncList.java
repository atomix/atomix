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
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.CompletableFuture;

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
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  static <T> AsyncList<T> create(String name, String uri) {
    return create(name, uri, new ClusterConfig(), new AsyncListConfig(String.format("copycat.list.%s", name)), ExecutionContext.create());
  }

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
    return create(name, uri, cluster, new AsyncListConfig(String.format("copycat.list.%s", name)), ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous list.
   *
   * @param name The asynchronous list name.
   * @param uri The asynchronous list member URI.
   * @param config The list configuration.
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  static <T> AsyncList<T> create(String name, String uri, AsyncListConfig config) {
    return create(name, uri, new ClusterConfig(), config, ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous list.
   *
   * @param name The asynchronous list name.
   * @param uri The asynchronous list member URI.
   * @param context The user execution context.
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  static <T> AsyncList<T> create(String name, String uri, ExecutionContext context) {
    return create(name, uri, new ClusterConfig(), new AsyncListConfig(String.format("copycat.list.%s", name)), context);
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
  static <T> AsyncList<T> create(String name, String uri, ClusterConfig cluster, AsyncListConfig config) {
    return create(name, uri, cluster, config, ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous list.
   *
   * @param name The asynchronous list name.
   * @param uri The asynchronous list member URI.
   * @param cluster The cluster configuration.
   * @param context The user execution context.
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  static <T> AsyncList<T> create(String name, String uri, ClusterConfig cluster, ExecutionContext context) {
    return create(name, uri, cluster, new AsyncListConfig(String.format("copycat.list.%s", name)), context);
  }

  /**
   * Creates a new asynchronous list.
   *
   * @param name The asynchronous list name.
   * @param uri The asynchronous list member URI.
   * @param config The list configuration.
   * @param context The user execution context.
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  static <T> AsyncList<T> create(String name, String uri, AsyncListConfig config, ExecutionContext context) {
    return create(name, uri, new ClusterConfig(), config, context);
  }

  /**
   * Creates a new asynchronous list.
   *
   * @param name The asynchronous list name.
   * @param uri The asynchronous list member URI.
   * @param cluster The cluster configuration.
   * @param config The list configuration.
   * @param context The user execution context.
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  @SuppressWarnings("unchecked")
  static <T> AsyncList<T> create(String name, String uri, ClusterConfig cluster, AsyncListConfig config, ExecutionContext context) {
    return new DefaultAsyncList(StateMachine.create(name, uri, AsyncListState.class, new DefaultAsyncListState<>(), cluster, config, context));
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
