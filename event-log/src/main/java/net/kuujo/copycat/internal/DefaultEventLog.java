/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal;

import net.kuujo.copycat.ActionOptions;
import net.kuujo.copycat.Coordinator;
import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.EventLog;
import net.kuujo.copycat.cluster.Cluster;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Event log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultEventLog<T> extends AbstractResource implements EventLog<T> {
  private Consumer<T> consumer;

  public DefaultEventLog(String name, Coordinator coordinator, Cluster cluster, CopycatContext context) {
    super(name, coordinator, cluster, context);
  }

  @Override
  public EventLog<T> consumer(Consumer<T> consumer) {
    this.consumer = consumer;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> get(long index) {
    CompletableFuture<T> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      try {
        future.complete(context.log().getEntry(index));
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Long> commit(T entry) {
    return context.submit("commit", entry);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> replay() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      if (consumer != null) {
        for (long i = context.log().firstIndex(); i <= context.log().lastIndex(); i++) {
          consumer.accept(context.log().getEntry(i));
        }
        future.complete(null);
      } else {
        future.completeExceptionally(new IllegalStateException("No consumer registered"));
      }
    });
    return future;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> replay(long index) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      if (consumer != null) {
        for (long i = index; i <= context.log().lastIndex(); i++) {
          consumer.accept(context.log().getEntry(i));
        }
        future.complete(null);
      } else {
        future.completeExceptionally(new IllegalStateException("No consumer registered"));
      }
    });
    return future;
  }

  /**
   * Handles a commit.
   */
  private long handleCommit(Long index, T entry) {
    return index;
  }

  @Override
  public CompletableFuture<Void> open() {
    return super.open().thenRunAsync(() -> {
      context.register("commit", this::handleCommit, new ActionOptions().withConsistent(true).withPersistent(true));
    }, context.executor());
  }

  @Override
  public CompletableFuture<Void> close() {
    context.unregister("commit");
    return super.close();
  }

}
