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

package net.kuujo.copycat.manager;

import net.kuujo.copycat.raft.protocol.error.ApplicationException;
import net.kuujo.copycat.raft.protocol.Operation;
import net.kuujo.copycat.raft.Commit;
import net.kuujo.copycat.util.Scheduled;
import net.kuujo.copycat.raft.StateMachineContext;
import net.kuujo.copycat.raft.StateMachineExecutor;
import net.kuujo.copycat.util.concurrent.ComposableFuture;
import net.kuujo.copycat.util.concurrent.Context;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Resource state machine executor.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ResourceStateMachineExecutor implements StateMachineExecutor {
  private final StateMachineExecutor parent;
  private final Context context;
  private final Map<Class, Function> operations = new HashMap<>();
  private final Set<Scheduled> tasks = new HashSet<>();
  private Function allOperation;

  ResourceStateMachineExecutor(StateMachineExecutor parent, Context context) {
    this.parent = parent;
    this.context = context;
  }

  @Override
  public StateMachineContext context() {
    return parent.context();
  }

  @Override
  public CompletableFuture<Void> execute(Runnable callback) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.execute(() -> {
      try {
        callback.run();
        future.complete(null);
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  @Override
  public <T> CompletableFuture<T> execute(Callable<T> callback) {
    CompletableFuture<T> future = new CompletableFuture<>();
    context.execute(() -> {
      try {
        future.complete(callback.call());
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Operation<U>, U> CompletableFuture<U> execute(Commit<T> commit) {
    ComposableFuture<U> future = new ComposableFuture<>();
    context.execute(() -> {
      // Get the function registered for the operation. If no function is registered, attempt to
      // use a global function if available.
      Function function = operations.get(commit.type());
      if (function == null) {
        function = allOperation;
      }

      if (function == null) {
        future.completeExceptionally(new IllegalStateException("unknown state machine operation: " + commit.type()));
      } else {
        // Execute the operation. If the operation return value is a Future, await the result,
        // otherwise immediately complete the execution future.
        try {
          Object result = function.apply(commit);
          if (result instanceof CompletableFuture) {
            ((CompletableFuture<U>) result).whenCompleteAsync(future, context);
          } else if (result instanceof Future) {
            future.complete(((Future<U>) result).get());
          } else {
            future.complete((U) result);
          }
        } catch (Exception e) {
          future.completeExceptionally(new ApplicationException("An application error occurred", e));
        }
      }
    });

    return future;
  }

  @Override
  public Scheduled schedule(Duration delay, Runnable callback) {
    Scheduled task = parent.schedule(delay, () -> context.execute(callback));
    tasks.add(task);
    return task;
  }

  @Override
  public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
    Scheduled task = parent.schedule(initialDelay, interval, () -> context.execute(callback));
    tasks.add(task);
    return task;
  }

  @Override
  public StateMachineExecutor register(Function<Commit<? extends Operation<?>>, ?> callback) {
    allOperation = callback;
    return this;
  }

  @Override
  public <T extends Operation<Void>> StateMachineExecutor register(Class<T> type, Consumer<Commit<T>> callback) {
    if (callback == null)
      throw new NullPointerException("callback cannot be null");
    operations.put(type, (Function<Commit<T>, Void>) commit -> {
      callback.accept(commit);
      return null;
    });
    return this;
  }

  @Override
  public <T extends Operation<U>, U> StateMachineExecutor register(Class<T> type, Function<Commit<T>, U> callback) {
    if (callback == null)
      throw new NullPointerException("callback cannot be null");
    operations.put(type, callback);
    return this;
  }

  @Override
  public void close() {
    tasks.forEach(Scheduled::cancel);
  }

}
