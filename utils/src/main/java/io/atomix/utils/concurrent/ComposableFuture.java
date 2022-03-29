// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Special implementation of {@link CompletableFuture} with missing utility methods.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ComposableFuture<T> extends CompletableFuture<T> implements BiConsumer<T, Throwable> {

  @Override
  public void accept(T result, Throwable error) {
    if (error == null) {
      complete(result);
    } else {
      completeExceptionally(error);
    }
  }

  /**
   * Sets a consumer to be called when the future is failed.
   *
   * @param consumer The consumer to call.
   * @return A new future.
   */
  public CompletableFuture<T> except(Consumer<Throwable> consumer) {
    return whenComplete((result, error) -> {
      if (error != null) {
        consumer.accept(error);
      }
    });
  }

  /**
   * Sets a consumer to be called asynchronously when the future is failed.
   *
   * @param consumer The consumer to call.
   * @return A new future.
   */
  public CompletableFuture<T> exceptAsync(Consumer<Throwable> consumer) {
    return whenCompleteAsync((result, error) -> {
      if (error != null) {
        consumer.accept(error);
      }
    });
  }

  /**
   * Sets a consumer to be called asynchronously when the future is failed.
   *
   * @param consumer The consumer to call.
   * @param executor The executor with which to call the consumer.
   * @return A new future.
   */
  public CompletableFuture<T> exceptAsync(Consumer<Throwable> consumer, Executor executor) {
    return whenCompleteAsync((result, error) -> {
      if (error != null) {
        consumer.accept(error);
      }
    }, executor);
  }

}
