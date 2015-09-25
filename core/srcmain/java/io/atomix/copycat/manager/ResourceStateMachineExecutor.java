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

package io.atomix.copycat.manager;

import io.atomix.catalogue.client.Operation;
import io.atomix.catalogue.server.Commit;
import io.atomix.catalogue.server.StateMachineContext;
import io.atomix.catalogue.server.StateMachineExecutor;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.ComposableFuture;
import io.atomix.catalyst.util.concurrent.Scheduled;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Resource state machine executor.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ResourceStateMachineExecutor implements StateMachineExecutor {
  private final StateMachineExecutor parent;
  private final ThreadContext context;
  private final Logger logger;
  private final Map<Class, Function> operations = new HashMap<>();
  private final Set<Scheduled> tasks = new HashSet<>();
  private Function allOperation;

  ResourceStateMachineExecutor(long resource, StateMachineExecutor parent, ThreadContext context) {
    this.parent = parent;
    this.context = context;
    this.logger = LoggerFactory.getLogger(String.format("%s-%d", getClass().getName(), resource));
  }

  @Override
  public StateMachineContext context() {
    return parent.context();
  }

  @Override
  public Logger logger() {
    return logger;
  }

  @Override
  public Serializer serializer() {
    return context.serializer();
  }

  @Override
  public Executor executor() {
    return context.executor();
  }

  @Override
  public CompletableFuture<Void> execute(Runnable callback) {
    return context.execute(callback);
  }

  @Override
  public <T> CompletableFuture<T> execute(Supplier<T> callback) {
    return context.execute(callback);
  }

  /**
   * Executes the given commit on the state machine.
   */
  @SuppressWarnings("unchecked")
  <T extends Operation<U>, U> CompletableFuture<U> execute(Commit<T> commit) {
    ThreadContext parent = ThreadContext.currentContext();

    ComposableFuture<U> future = new ComposableFuture<>();
    context.executor().execute(() -> {
      // Get the function registered for the operation. If no function is registered, attempt to
      // use a global function if available.
      Function function = operations.get(commit.type());
      if (function == null) {
        function = allOperation;
      }

      if (function == null) {
        parent.executor().execute(() -> future.completeExceptionally(new IllegalStateException("unknown state machine operation: " + commit.type())));
      } else {
        // Execute the operation. If the operation return value is a Future, await the result,
        // otherwise immediately complete the execution future.
        Object result = function.apply(commit);
        if (result instanceof CompletableFuture) {
          ((CompletableFuture<U>) result).whenCompleteAsync(future, parent.executor());
        } else if (result instanceof Future) {
          parent.executor().execute(() -> {
            try {
              future.complete(((Future<U>) result).get());
            } catch (ExecutionException | InterruptedException e) {
              throw new RuntimeException(e);
            }
          });
        } else {
          parent.executor().execute(() -> future.complete((U) result));
        }
      }
    });

    return future;
  }

  @Override
  public Scheduled schedule(Duration delay, Runnable callback) {
    Scheduled task = parent.schedule(delay, () -> context.executor().execute(callback));
    tasks.add(task);
    return task;
  }

  @Override
  public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
    Scheduled task = parent.schedule(initialDelay, interval, () -> context.executor().execute(callback));
    tasks.add(task);
    return task;
  }

  @Override
  public StateMachineExecutor register(Function<Commit<? extends Operation<?>>, ?> callback) {
    allOperation = Assert.notNull(callback, "callback");
    return this;
  }

  @Override
  public <T extends Operation<Void>> StateMachineExecutor register(Class<T> type, Consumer<Commit<T>> callback) {
    Assert.notNull(type, "type");
    Assert.notNull(callback, "callback");
    operations.put(type, (Function<Commit<T>, Void>) commit -> {
      callback.accept(commit);
      return null;
    });
    return this;
  }

  @Override
  public <T extends Operation<?>> StateMachineExecutor register(Class<T> type, Function<Commit<T>, ?> callback) {
    Assert.notNull(type, "type");
    Assert.notNull(callback, "callback");
    operations.put(type, callback);
    return this;
  }

  @Override
  public void close() {
    tasks.forEach(Scheduled::cancel);
  }

}
