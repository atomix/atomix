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
package io.atomix.util.concurrent;

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
     * Returns a future that's completed using the given {@code orderedExecutor} if the future is not blocked or the
     * given {@code threadPoolExecutor} if the future is blocked.
     * <p>
     * This method allows futures to maintain single-thread semantics via the provided {@code orderedExecutor} while
     * ensuring user code can block without blocking completion of futures. When the returned future or any of its
     * descendants is blocked on a {@link CompletableFuture#get()} or {@link CompletableFuture#join()} call, completion
     * of the returned future will be done using the provided {@code threadPoolExecutor}.
     *
     * @param future             the future to convert into an asynchronous future
     * @param orderedExecutor    the ordered executor with which to attempt to complete the future
     * @param threadPoolExecutor the backup executor with which to complete blocked futures
     * @param <T>                future value type
     * @return a new completable future to be completed using the provided {@code executor} once the provided
     * {@code future} is complete
     */
    public static <T> CompletableFuture<T> blockingAwareFuture(
            CompletableFuture<T> future,
            Executor orderedExecutor,
            Executor threadPoolExecutor) {
        if (future.isDone()) {
            return future;
        }

        BlockingAwareFuture<T> newFuture = new BlockingAwareFuture<T>();
        future.whenComplete((result, error) -> {
            Runnable completer = () -> {
                if (future.isCompletedExceptionally()) {
                    newFuture.completeExceptionally(error);
                } else {
                    newFuture.complete(result);
                }
            };

            if (newFuture.isBlocked()) {
                threadPoolExecutor.execute(completer);
            } else {
                orderedExecutor.execute(completer);
            }
        });
        return newFuture;
    }

}
