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

import net.kuujo.copycat.Coordinator;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.internal.DefaultAsyncList;
import net.kuujo.copycat.internal.DefaultCoordinator;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  static <T> AsyncList<T> create(String name) {
    return create(name, Services.load("cluster"), Services.load("protocol"));
  }

  /**
   * Creates a new asynchronous list.
   *
   * @param name The asynchronous list name.
   * @param config The cluster configuration.
   * @param protocol The cluster protocol.
   * @param <T> The list data type.
   * @return The asynchronous list.
   */
  @SuppressWarnings("unchecked")
  static <T> AsyncList<T> create(String name, ClusterConfig config, Protocol protocol) {
    ExecutionContext executor = ExecutionContext.create();
    Coordinator coordinator = new DefaultCoordinator(config, protocol, new InMemoryLog(), executor);
    try {
      return coordinator.<AsyncList<T>>createResource(name, resource -> new InMemoryLog(), (resource, coord, cluster, context) -> {
        return (AsyncList<T>) new DefaultAsyncList<T>(resource, coord, cluster, context).withShutdownTask(coordinator::close);
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Gets a value at a specific index in the list.
   *
   * @param index The index of the value to get.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<T> get(int index);

  /**
   * Sets an index in the list.
   *
   * @param index The index to set.
   * @param value The value to set.
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
