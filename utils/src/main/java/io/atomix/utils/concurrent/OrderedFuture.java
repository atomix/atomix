/*
 * Copyright 2017-present Open Networking Foundation
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

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link CompletableFuture} that ensures callbacks are called in FIFO order.
 * <p>
 * The default {@link CompletableFuture} does not guarantee the ordering of callbacks, and indeed appears to
 * execute them in LIFO order.
 */
public class OrderedFuture<T> extends CompletableFuture<T> {
  private final Queue<CompletableFuture<T>> orderedFutures = new LinkedList<>();
  private boolean complete;
  private T result;
  private Throwable error;

  public OrderedFuture() {
    super.whenComplete(this::complete);
  }

  /**
   * Adds a new ordered future.
   */
  private CompletableFuture<T> orderedFuture() {
    synchronized (orderedFutures) {
      if (complete) {
        if (error == null) {
          return CompletableFuture.completedFuture(result);
        } else {
          return Futures.exceptionalFuture(error);
        }
      } else {
        CompletableFuture<T> future = new CompletableFuture<>();
        orderedFutures.add(future);
        return future;
      }
    }
  }

  /**
   * Completes futures in FIFO order.
   */
  private void complete(T result, Throwable error) {
    synchronized (orderedFutures) {
      this.complete = true;
      this.result = result;
      this.error = error;
      if (error == null) {
        for (CompletableFuture<T> future : orderedFutures) {
          future.complete(result);
        }
      } else {
        for (CompletableFuture<T> future : orderedFutures) {
          future.completeExceptionally(error);
        }
      }
    }
  }

  @Override
  public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
    return orderedFuture().thenApply(fn);
  }

  @Override
  public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
    return orderedFuture().thenApplyAsync(fn);
  }

  @Override
  public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
    return orderedFuture().thenApplyAsync(fn, executor);
  }

  @Override
  public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
    return orderedFuture().thenAccept(action);
  }

  @Override
  public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
    return orderedFuture().thenAcceptAsync(action);
  }

  @Override
  public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
    return orderedFuture().thenAcceptAsync(action, executor);
  }

  @Override
  public CompletableFuture<Void> thenRun(Runnable action) {
    return orderedFuture().thenRun(action);
  }

  @Override
  public CompletableFuture<Void> thenRunAsync(Runnable action) {
    return orderedFuture().thenRunAsync(action);
  }

  @Override
  public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
    return orderedFuture().thenRunAsync(action, executor);
  }

  @Override
  public <U, V> CompletableFuture<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
    return orderedFuture().thenCombine(other, fn);
  }

  @Override
  public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
    return orderedFuture().thenCombineAsync(other, fn);
  }

  @Override
  public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
    return orderedFuture().thenCombineAsync(other, fn, executor);
  }

  @Override
  public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
    return orderedFuture().thenAcceptBoth(other, action);
  }

  @Override
  public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
    return orderedFuture().thenAcceptBothAsync(other, action);
  }

  @Override
  public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor) {
    return orderedFuture().thenAcceptBothAsync(other, action, executor);
  }

  @Override
  public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
    return orderedFuture().runAfterBoth(other, action);
  }

  @Override
  public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
    return orderedFuture().runAfterBothAsync(other, action);
  }

  @Override
  public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
    return orderedFuture().runAfterBothAsync(other, action, executor);
  }

  @Override
  public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
    return orderedFuture().applyToEither(other, fn);
  }

  @Override
  public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
    return orderedFuture().applyToEitherAsync(other, fn);
  }

  @Override
  public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
    return orderedFuture().applyToEitherAsync(other, fn, executor);
  }

  @Override
  public CompletableFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
    return orderedFuture().acceptEither(other, action);
  }

  @Override
  public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
    return orderedFuture().acceptEitherAsync(other, action);
  }

  @Override
  public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
    return orderedFuture().acceptEitherAsync(other, action, executor);
  }

  @Override
  public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
    return orderedFuture().runAfterEither(other, action);
  }

  @Override
  public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
    return orderedFuture().runAfterEitherAsync(other, action);
  }

  @Override
  public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
    return orderedFuture().runAfterEitherAsync(other, action, executor);
  }

  @Override
  public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
    return orderedFuture().thenCompose(fn);
  }

  @Override
  public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
    return orderedFuture().thenComposeAsync(fn);
  }

  @Override
  public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
    return orderedFuture().thenComposeAsync(fn, executor);
  }

  @Override
  public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
    return orderedFuture().whenComplete(action);
  }

  @Override
  public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
    return orderedFuture().whenCompleteAsync(action);
  }

  @Override
  public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
    return orderedFuture().whenCompleteAsync(action, executor);
  }

  @Override
  public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
    return orderedFuture().handle(fn);
  }

  @Override
  public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
    return orderedFuture().handleAsync(fn);
  }

  @Override
  public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
    return orderedFuture().handleAsync(fn, executor);
  }

  @Override
  public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
    return orderedFuture().exceptionally(fn);
  }

  @Override
  public CompletableFuture<T> toCompletableFuture() {
    return this;
  }
}
