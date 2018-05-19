/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.utils.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Utilities for creating completed and exceptional futures.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class Futures {

  /**
   * Creates a future that is synchronously completed.
   *
   * @param result The future result.
   * @return The completed future.
   */
  public static <T> CompletableFuture<T> completedFuture(T result) {
    return CompletableFuture.completedFuture(result);
  }

  /**
   * Creates a future that is asynchronously completed.
   *
   * @param result   The future result.
   * @param executor The executor on which to complete the future.
   * @return The completed future.
   */
  public static <T> CompletableFuture<T> completedFutureAsync(T result, Executor executor) {
    CompletableFuture<T> future = new CompletableFuture<>();
    executor.execute(() -> future.complete(result));
    return future;
  }

  /**
   * Creates a future that is synchronously completed exceptionally.
   *
   * @param t The future exception.
   * @return The exceptionally completed future.
   */
  public static <T> CompletableFuture<T> exceptionalFuture(Throwable t) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(t);
    return future;
  }

  /**
   * Creates a future that is asynchronously completed exceptionally.
   *
   * @param t        The future exception.
   * @param executor The executor on which to complete the future.
   * @return The exceptionally completed future.
   */
  public static <T> CompletableFuture<T> exceptionalFutureAsync(Throwable t, Executor executor) {
    CompletableFuture<T> future = new CompletableFuture<>();
    executor.execute(() -> {
      future.completeExceptionally(t);
    });
    return future;
  }

  /**
   * Returns a future that completes callbacks in add order.
   *
   * @param <T> future value type
   * @return a new completable future that will complete added callbacks in the order in which they were added
   */
  public static <T> CompletableFuture<T> orderedFuture() {
    return new OrderedFuture<>();
  }

  /**
   * Returns a wrapped future that will be completed on the given executor.
   *
   * @param future   the future to be completed on the given executor
   * @param executor the executor with which to complete the future
   * @param <T>      the future value type
   * @return a wrapped future to be completed on the given executor
   */
  public static <T> CompletableFuture<T> asyncFuture(CompletableFuture<T> future, Executor executor) {
    CompletableFuture<T> newFuture = new AtomixFuture<>();
    future.whenComplete((result, error) -> {
      executor.execute(() -> {
        if (error == null) {
          newFuture.complete(result);
        } else {
          newFuture.completeExceptionally(error);
        }
      });
    });
    return newFuture;
  }
}
