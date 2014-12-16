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

import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.EventLog;
import net.kuujo.copycat.EventLogConfig;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.util.serializer.Serializer;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Event log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultEventLog<T> extends AbstractCopycatResource<EventLog<T>> implements EventLog<T> {
  private final Serializer serializer;
  private Consumer<T> consumer;

  public DefaultEventLog(String name, CopycatContext context, EventLogConfig config, ExecutionContext executor) {
    super(name, context, executor);
    context.log().config().withSegmentSize(config.getSegmentSize())
      .withSegmentInterval(config.getSegmentInterval())
      .withFlushOnWrite(config.isFlushOnWrite())
      .withFlushInterval(config.getFlushInterval())
      .withRetentionPolicy(config.getRetentionPolicy());
    this.serializer = config.getSerializer();
  }

  @Override
  public EventLog<T> consumer(Consumer<T> consumer) {
    this.consumer = consumer;
    return this;
  }

  @Override
  public <U extends T> CompletableFuture<U> get(long index) {
    CompletableFuture<U> future = new CompletableFuture<>();
    context.execute(() -> {
      if (!context.log().containsIndex(index)) {
        executor.execute(() -> {
          future.completeExceptionally(new IndexOutOfBoundsException(String.format("Log index %d out of bounds", index)));
        });
      } else {
        ByteBuffer buffer = context.log().getEntry(index);
        if (buffer != null) {
          U entry = serializer.readObject(buffer);
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

  @Override
  public CompletableFuture<Void> replay() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.execute(() -> replay(context.log().firstIndex(), future));
    return future;
  }

  @Override
  public CompletableFuture<Void> replay(long index) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.execute(() -> replay(index, future));
    return future;
  }

  /**
   * Handles asynchronous replay of the log. The log is read on the internal context and the consumer
   * is called on the user's context.
   */
  private void replay(long index, CompletableFuture<Void> future) {
    if (context.log().containsIndex(index)) {
      executor.execute(() -> {
        consumer.accept(serializer.readObject(context.log().getEntry(index)));
        executor.execute(() -> replay(index, future));
      });
    } else {
      executor.execute(() -> future.complete(null));
    }
  }

  /**
   * Handles a log write.
   */
  private ByteBuffer consume(Long index, ByteBuffer entry) {
    ByteBuffer result = ByteBuffer.allocateDirect(8);
    result.putLong(index);
    if (consumer != null) {
      consumer.accept(serializer.readObject(entry));
    }
    return result;
  }

  @Override
  public CompletableFuture<Void> open() {
    return super.open().thenRunAsync(() -> {
      context.consumer(this::consume);
    }, executor);
  }

  @Override
  public CompletableFuture<Void> close() {
    context.consumer(null);
    return super.close();
  }

}
