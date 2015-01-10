/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.EventLog;
import net.kuujo.copycat.ResourceContext;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Default event log partition implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultEventLog<T> extends AbstractResource<EventLog<T>> implements EventLog<T> {
  private EventListener<T> consumer;

  public DefaultEventLog(ResourceContext context) {
    super(context);
    context.consumer(this::consume);
  }

  @Override
  public EventLog<T> consumer(EventListener<T> consumer) {
    this.consumer = consumer;
    return this;
  }

  @Override
  public CompletableFuture<T> get(long index) {
    CompletableFuture<T> future = new CompletableFuture<>();
    context.execute(() -> {
      if (!context.log().containsIndex(index)) {
        executor.execute(() -> future.completeExceptionally(new IndexOutOfBoundsException(String.format("Log index %d out of bounds", index))));
      } else {
        ByteBuffer buffer = context.log().getEntry(index);
        if (buffer != null) {
          T entry = serializer.readObject(buffer);
          executor.execute(() -> future.complete(entry));
        } else {
          executor.execute(() -> future.complete(null));
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Long> commit(T entry) {
    return context.commit(serializer.writeObject(entry)).thenApplyAsync(ByteBuffer::getLong, executor);
  }

  /**
   * Handles a log write.
   */
  private ByteBuffer consume(Long index, ByteBuffer entry) {
    ByteBuffer result = ByteBuffer.allocateDirect(8);
    result.putLong(index);
    if (consumer != null) {
      T value = serializer.readObject(entry);
      executor.execute(() -> consumer.handle(value));
    }
    return result;
  }

  @Override
  public CompletableFuture<EventLog<T>> open() {
    return runStartupTasks()
      .thenComposeAsync(v -> context.open(), executor)
      .thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return context.close()
      .thenCompose(v -> runShutdownTasks());
  }

}
