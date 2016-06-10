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

package io.atomix.manager.internal;

import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Operation;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachineExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Resource state machine executor.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ResourceManagerStateMachineExecutor implements StateMachineExecutor {
  final StateMachineExecutor parent;
  final ResourceManagerStateMachineContext context;
  private final Logger logger;
  private final Map<Class, Function> operations = new HashMap<>();
  private final Set<Scheduled> tasks = new HashSet<>();

  ResourceManagerStateMachineExecutor(long resource, StateMachineExecutor parent) {
    this.parent = parent;
    this.context = new ResourceManagerStateMachineContext(parent.context());
    this.logger = LoggerFactory.getLogger(String.format("%s-%d", getClass().getName(), resource));
  }

  @Override
  public ResourceManagerStateMachineContext context() {
    return context;
  }

  @Override
  public Logger logger() {
    return logger;
  }

  @Override
  public Serializer serializer() {
    return parent.serializer();
  }

  @Override
  public Executor executor() {
    return parent.executor();
  }

  @Override
  public CompletableFuture<Void> execute(Runnable callback) {
    return parent.execute(callback);
  }

  @Override
  public <T> CompletableFuture<T> execute(Supplier<T> callback) {
    return parent.execute(callback);
  }

  /**
   * Executes the given commit on the state machine.
   */
  @SuppressWarnings("unchecked")
  <T extends Operation<U>, U> U execute(Commit<T> commit) {
    // Get the function registered for the operation. If no function is registered, attempt to
    // use a global function if available.
    Function function = operations.get(commit.type());

    if (function == null) {
      throw new IllegalStateException("unknown state machine operation: " + commit.type());
    } else {
      // Execute the operation. If the operation return value is a Future, await the result,
      // otherwise immediately complete the execution future.
      return (U) function.apply(commit);
    }
  }

  @Override
  public Scheduled schedule(Duration delay, Runnable callback) {
    Scheduled task = parent.schedule(delay, callback);
    tasks.add(task);
    return task;
  }

  @Override
  public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
    Scheduled task = parent.schedule(initialDelay, interval, callback);
    tasks.add(task);
    return task;
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
  public <T extends Operation<U>, U> StateMachineExecutor register(Class<T> type, Function<Commit<T>, U> callback) {
    Assert.notNull(type, "type");
    Assert.notNull(callback, "callback");
    operations.put(type, callback);
    return this;
  }

  @Override
  public void close() {
    tasks.forEach(Scheduled::cancel);
    context.close();
  }

}
