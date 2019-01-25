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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  /**
   * Wraps the given future in a new blockable future.
   *
   * @param future the future to wrap
   * @param <T>    the future value type
   * @return a new blockable future
   */
  public static <T> CompletableFuture<T> wrap(CompletableFuture<T> future) {
    CompletableFuture<T> newFuture = new OrderedFuture<>();
    future.whenComplete((result, error) -> {
      if (error == null) {
        newFuture.complete(result);
      } else {
        newFuture.completeExceptionally(error);
      }
    });
    return newFuture;
  }

  private static final ThreadContext NULL_CONTEXT = new NullThreadContext();

  private final Queue<CompletableFuture<T>> orderedFutures = new LinkedList<>();
  private volatile boolean complete;
  private volatile T result;
  private volatile Throwable error;

  public OrderedFuture() {
    super.whenComplete(this::complete);
  }

  private ThreadContext getThreadContext() {
    ThreadContext context = ThreadContext.currentContext();
    return context != null ? context : NULL_CONTEXT;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    ThreadContext context = getThreadContext();
    context.block();
    try {
      return super.get();
    } finally {
      context.unblock();
    }
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    ThreadContext context = getThreadContext();
    context.block();
    try {
      return super.get(timeout, unit);
    } finally {
      context.unblock();
    }
  }

  @Override
  public synchronized T join() {
    ThreadContext context = getThreadContext();
    context.block();
    try {
      return super.join();
    } finally {
      context.unblock();
    }
  }

  /**
   * Adds a new ordered future.
   */
  private CompletableFuture<T> orderedFuture() {
    if (!complete) {
      synchronized (orderedFutures) {
        if (!complete) {
          CompletableFuture<T> future = new CompletableFuture<>();
          orderedFutures.add(future);
          return future;
        }
      }
    }

    // Completed
    if (error == null) {
      return CompletableFuture.completedFuture(result);
    } else {
      return Futures.exceptionalFuture(error);
    }
  }

  /**
   * Completes futures in FIFO order.
   */
  private void complete(T result, Throwable error) {
    synchronized (orderedFutures) {
      this.result = result;
      this.error = error;
      this.complete = true;
      if (error == null) {
        for (CompletableFuture<T> future : orderedFutures) {
          future.complete(result);
        }
      } else {
        for (CompletableFuture<T> future : orderedFutures) {
          future.completeExceptionally(error);
        }
      }
      orderedFutures.clear();
    }
  }

  @Override
  public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
    return wrap(orderedFuture().thenApply(fn));
  }

  @Override
  public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
    return wrap(orderedFuture().thenApplyAsync(fn));
  }

  @Override
  public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
    return wrap(orderedFuture().thenApplyAsync(fn, executor));
  }

  @Override
  public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
    return wrap(orderedFuture().thenAccept(action));
  }

  @Override
  public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
    return wrap(orderedFuture().thenAcceptAsync(action));
  }

  @Override
  public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
    return wrap(orderedFuture().thenAcceptAsync(action, executor));
  }

  @Override
  public CompletableFuture<Void> thenRun(Runnable action) {
    return wrap(orderedFuture().thenRun(action));
  }

  @Override
  public CompletableFuture<Void> thenRunAsync(Runnable action) {
    return wrap(orderedFuture().thenRunAsync(action));
  }

  @Override
  public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
    return wrap(orderedFuture().thenRunAsync(action, executor));
  }

  @Override
  public <U, V> CompletableFuture<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
    return wrap(orderedFuture().thenCombine(other, fn));
  }

  @Override
  public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
    return wrap(orderedFuture().thenCombineAsync(other, fn));
  }

  @Override
  public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
    return wrap(orderedFuture().thenCombineAsync(other, fn, executor));
  }

  @Override
  public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
    return wrap(orderedFuture().thenAcceptBoth(other, action));
  }

  @Override
  public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
    return wrap(orderedFuture().thenAcceptBothAsync(other, action));
  }

  @Override
  public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor) {
    return wrap(orderedFuture().thenAcceptBothAsync(other, action, executor));
  }

  @Override
  public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
    return wrap(orderedFuture().runAfterBoth(other, action));
  }

  @Override
  public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
    return wrap(orderedFuture().runAfterBothAsync(other, action));
  }

  @Override
  public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
    return wrap(orderedFuture().runAfterBothAsync(other, action, executor));
  }

  @Override
  public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
    return wrap(orderedFuture().applyToEither(other, fn));
  }

  @Override
  public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
    return wrap(orderedFuture().applyToEitherAsync(other, fn));
  }

  @Override
  public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
    return wrap(orderedFuture().applyToEitherAsync(other, fn, executor));
  }

  @Override
  public CompletableFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
    return wrap(orderedFuture().acceptEither(other, action));
  }

  @Override
  public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
    return wrap(orderedFuture().acceptEitherAsync(other, action));
  }

  @Override
  public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
    return wrap(orderedFuture().acceptEitherAsync(other, action, executor));
  }

  @Override
  public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
    return wrap(orderedFuture().runAfterEither(other, action));
  }

  @Override
  public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
    return wrap(orderedFuture().runAfterEitherAsync(other, action));
  }

  @Override
  public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
    return wrap(orderedFuture().runAfterEitherAsync(other, action, executor));
  }

  @Override
  public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
    return wrap(orderedFuture().thenCompose(fn));
  }

  @Override
  public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
    return wrap(orderedFuture().thenComposeAsync(fn));
  }

  @Override
  public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
    return wrap(orderedFuture().thenComposeAsync(fn, executor));
  }

  @Override
  public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
    return wrap(orderedFuture().whenComplete(action));
  }

  @Override
  public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
    return wrap(orderedFuture().whenCompleteAsync(action));
  }

  @Override
  public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
    return wrap(orderedFuture().whenCompleteAsync(action, executor));
  }

  @Override
  public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
    return wrap(orderedFuture().handle(fn));
  }

  @Override
  public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
    return wrap(orderedFuture().handleAsync(fn));
  }

  @Override
  public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
    return wrap(orderedFuture().handleAsync(fn, executor));
  }

  @Override
  public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
    return wrap(orderedFuture().exceptionally(fn));
  }

  @Override
  public CompletableFuture<T> toCompletableFuture() {
    return this;
  }
}
