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

import net.kuujo.copycat.CopycatCoordinator;
import net.kuujo.copycat.EventLog;
import net.kuujo.copycat.EventLogConfig;
import net.kuujo.copycat.log.ChronicleLog;
import net.kuujo.copycat.log.LogConfig;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Event log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultEventLog extends AbstractCopycatResource implements EventLog {
  private Consumer<ByteBuffer> consumer;

  public DefaultEventLog(String name, CopycatCoordinator coordinator, EventLogConfig config) {
    super(name, coordinator, resource -> new ChronicleLog(name, new LogConfig()
      .withDirectory(config.getDirectory())
      .withSegmentSize(config.getSegmentSize())
      .withSegmentInterval(config.getSegmentInterval())
      .withFlushOnWrite(config.isFlushOnWrite())
      .withFlushInterval(config.getFlushInterval())
      .withRetentionPolicy(config.getRetentionPolicy())));
  }

  @Override
  public EventLog consumer(Consumer<ByteBuffer> consumer) {
    this.consumer = consumer;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<ByteBuffer> get(long index) {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
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
  public CompletableFuture<Long> commit(ByteBuffer entry) {
    return context.commit(entry).thenApply(ByteBuffer::getLong);
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
   * Handles a log write.
   */
  private ByteBuffer consume(Long index, ByteBuffer entry) {
    ByteBuffer result = ByteBuffer.allocateDirect(8);
    result.putLong(index);
    return result;
  }

  @Override
  public CompletableFuture<Void> open() {
    return super.open().thenRunAsync(() -> {
      context.consumer(this::consume);
    }, context.executor());
  }

  @Override
  public CompletableFuture<Void> close() {
    context.consumer(null);
    return super.close();
  }

}
