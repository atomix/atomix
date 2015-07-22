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
package net.kuujo.copycat.util.concurrent;

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
   * @param result The future result.
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
   * @param t The future exception.
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

}
