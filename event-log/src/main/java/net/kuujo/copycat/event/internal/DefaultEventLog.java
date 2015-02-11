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
package net.kuujo.copycat.event.internal;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.event.EventLog;
import net.kuujo.copycat.event.EventLogConfig;
import net.kuujo.copycat.log.LogSegment;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.resource.internal.ResourceManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Default event log partition implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultEventLog<T> extends AbstractResource<EventLog<T>> implements EventLog<T> {
  private EventListener<T> consumer;
  private ScheduledFuture<?> retentionFuture;
  private Long commitIndex;

  public DefaultEventLog(ResourceManager context) {
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
  private ByteBuffer consume(long term, Long index, ByteBuffer entry) {
    ByteBuffer result = ByteBuffer.allocateDirect(8);
    result.putLong(index);
    if (consumer != null && entry != null) {
      T value = serializer.readObject(entry);
      executor.execute(() -> consumer.accept(value));
    }
    commitIndex = index;
    result.flip();
    return result;
  }

  /**
   * Compacts the log.
   */
  private synchronized void compact() {
    if (commitIndex != null) {
      // Iterate through segments in the log and remove/close/delete segments that should no longer be retained.
      // A segment is no longer retained if all of the following conditions are met:
      // - The segment is not the last segment in the log
      // - The segment's last index is less than or equal to the commit index
      // - The configured retention policy's retain(LogSegment) method returns false.
      for (Iterator<Map.Entry<Long, LogSegment>> iterator = context.log().segments().entrySet().iterator(); iterator.hasNext(); ) {
        Map.Entry<Long, LogSegment> entry = iterator.next();
        LogSegment segment = entry.getValue();
        if (context.log().lastSegment() != segment
          && segment.lastIndex() != null
          && segment.lastIndex() <= commitIndex
          && !context.config().<EventLogConfig>getResourceConfig().getRetentionPolicy().retain(entry.getValue())) {
          iterator.remove();
          try {
            segment.close();
            segment.delete();
          } catch (IOException e) {
          }
        }
      }
    }
  }

  @Override
  public synchronized CompletableFuture<EventLog<T>> open() {
    return runStartupTasks()
      .thenComposeAsync(v -> context.open(), executor)
      .thenRun(() -> {
        retentionFuture = context.scheduleWithFixedDelay(this::compact, 0, context.config().<EventLogConfig>getResourceConfig().getRetentionCheckInterval(), TimeUnit.MILLISECONDS);
      })
      .thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (retentionFuture != null) {
      retentionFuture.cancel(false);
    }
    return context.close()
      .thenCompose(v -> runShutdownTasks());
  }

}
