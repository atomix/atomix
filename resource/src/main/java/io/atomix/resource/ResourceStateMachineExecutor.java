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
 * limitations under the License
 */
package io.atomix.resource;

import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Operation;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachineContext;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.resource.internal.ResourceCommand;
import io.atomix.resource.internal.ResourceCommit;
import io.atomix.resource.internal.ResourceOperation;
import io.atomix.resource.internal.ResourceQuery;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;

/**
 * Custom executor for executing {@link ResourceStateMachine} {@link ResourceOperation operations}.
 * <p>
 * This is a special executor used by Atomix to execute resource state machines to unwrap
 * resource {@link ResourceCommand commands} and {@link ResourceQuery queries}.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ResourceStateMachineExecutor implements StateMachineExecutor {
  private final StateMachineExecutor parent;
  private final Map<Class, Function> callbacks = new HashMap<>();

  ResourceStateMachineExecutor(StateMachineExecutor parent) {
    this.parent = Assert.notNull(parent, "parent");
    parent.register(ResourceCommand.class, (Function<Commit<ResourceCommand>, Object>) this::executeCommand);
    parent.register(ResourceQuery.class, (Function<Commit<ResourceQuery>, Object>) this::executeQuery);
  }

  @Override
  public StateMachineContext context() {
    return parent.context();
  }

  @Override
  public Logger logger() {
    return parent.logger();
  }

  @Override
  public Serializer serializer() {
    return parent.serializer();
  }

  @Override
  public Executor executor() {
    return parent.executor();
  }

  /**
   * Executes a resource command.
   */
  @SuppressWarnings("unchecked")
  private Object executeCommand(Commit<ResourceCommand> commit) {
    Function<Commit<?>, ?> function = callbacks.get(commit.operation().operation().getClass());
    if (function != null) {
      return function.apply(new ResourceCommit(commit));
    }
    throw new IllegalStateException("unknown operation type: " + commit.operation().operation().getClass());
  }

  /**
   * Executes a resource query.
   */
  @SuppressWarnings("unchecked")
  private Object executeQuery(Commit<ResourceQuery> commit) {
    Function<Commit<?>, ?> function = callbacks.get(commit.operation().operation().getClass());
    if (function != null) {
      return function.apply(new ResourceCommit(commit));
    }
    throw new IllegalStateException("unknown operation type: " + commit.operation().operation().getClass());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Operation<Void>> StateMachineExecutor register(Class<T> type, Consumer<Commit<T>> callback) {
    return register(type, (Function<Commit<T>, Void>) c -> {
      callback.accept(c);
      return null;
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Operation<U>, U> StateMachineExecutor register(Class<T> type, Function<Commit<T>, U> callback) {
    callbacks.put(type, Assert.notNull(callback, "callback"));
    return this;
  }

  @Override
  public Scheduled schedule(Duration delay, Runnable callback) {
    return parent.schedule(delay, callback);
  }

  @Override
  public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
    return parent.schedule(initialDelay, interval, callback);
  }

}
