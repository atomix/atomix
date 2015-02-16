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
package net.kuujo.copycat.event;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.LogSegment;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Copycat event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventLog<T> extends AbstractResource<EventLog<T>> {

  /**
   * Creates a new event log, loading the log configuration from the classpath.
   *
   * @param <T> The event log entry type.
   * @return A new event log instance.
   */
  public static <T> EventLog<T> create() {
    return create(new EventLogConfig(), new ClusterConfig());
  }

  /**
   * Creates a new event log, loading the log configuration from the classpath.
   *
   * @param <T> The event log entry type.
   * @return A new event log instance.
   */
  public static <T> EventLog<T> create(Executor executor) {
    return create(new EventLogConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new event log, loading the log configuration from the classpath.
   *
   * @param name The event log resource name to be used to load the event log configuration from the classpath.
   * @param <T> The event log entry type.
   * @return A new event log instance.
   */
  public static <T> EventLog<T> create(String name) {
    return create(new EventLogConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new event log, loading the log configuration from the classpath.
   *
   * @param name The event log resource name to be used to load the event log configuration from the classpath.
   * @param executor An executor on which to execute event log callbacks.
   * @param <T> The event log entry type.
   * @return A new event log instance.
   */
  public static <T> EventLog<T> create(String name, Executor executor) {
    return create(new EventLogConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new event log with the given cluster and event log configurations.
   *
   * @param name The event log resource name to be used to load the event log configuration from the classpath.
   * @param cluster The cluster configuration.
   * @return A new event log instance.
   */
  public static <T> EventLog<T> create(String name, ClusterConfig cluster) {
    return create(new EventLogConfig(name), cluster);
  }

  /**
   * Creates a new event log with the given cluster and event log configurations.
   *
   * @param name The event log resource name to be used to load the event log configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute event log callbacks.
   * @return A new event log instance.
   */
  public static <T> EventLog<T> create(String name, ClusterConfig cluster, Executor executor) {
    return create(new EventLogConfig(name), cluster, executor);
  }

  /**
   * Creates a new event log with the given cluster and event log configurations.
   *
   * @param config The event log configuration.
   * @param cluster The cluster configuration.
   * @return A new event log instance.
   */
  public static <T> EventLog<T> create(EventLogConfig config, ClusterConfig cluster) {
    return new EventLog<>(config, cluster);
  }

  /**
   * Creates a new event log with the given cluster and event log configurations.
   *
   * @param config The event log configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute event log callbacks.
   * @return A new event log instance.
   */
  public static <T> EventLog<T> create(EventLogConfig config, ClusterConfig cluster, Executor executor) {
    return new EventLog<>(config, cluster, executor);
  }
  private EventListener<T> consumer;
  private ScheduledFuture<?> retentionFuture;
  private Long commitIndex;

  public EventLog(EventLogConfig config, ClusterConfig cluster) {
    this(new ResourceContext(config, cluster));
  }

  public EventLog(EventLogConfig config, ClusterConfig cluster, Executor executor) {
    this(new ResourceContext(config, cluster, executor));
  }

  public EventLog(ResourceContext context) {
    super(context);
    context.commitHandler(this::commit);
  }

  /**
   * Registers a log entry consumer.
   *
   * @param consumer The log entry consumer.
   * @return The event log.
   */
  public EventLog<T> consumer(EventListener<T> consumer) {
    this.consumer = consumer;
    return this;
  }

  /**
   * Gets an entry from the log.
   *
   * @param index The index from which to get the entry.
   * @return A completable future to be completed with the retrieved entry.
   */
  public CompletableFuture<T> get(long index) {
    CompletableFuture<T> future = new CompletableFuture<>();
    context.scheduler().execute(() -> {
      if (!context.raft().log().containsIndex(index)) {
        context.executor().execute(() -> future.completeExceptionally(new IndexOutOfBoundsException(String.format("Log index %d out of bounds", index))));
      } else {
        ByteBuffer buffer = context.raft().log().getEntry(index);
        if (buffer != null) {
          T entry = serializer.readObject(buffer);
          context.executor().execute(() -> future.complete(entry));
        } else {
          context.executor().execute(() -> future.complete(null));
        }
      }
    });
    return future;
  }

  /**
   * Commits an entry to the log.
   *
   * @param entry The entry to commit.
   * @return A completable future to be completed once the entry has been committed.
   */
  public CompletableFuture<Long> commit(T entry) {
    return context.commit(serializer.writeObject(entry)).thenApplyAsync(ByteBuffer::getLong, context.executor());
  }

  /**
   * Handles a log write.
   */
  private ByteBuffer commit(long term, Long index, ByteBuffer entry) {
    ByteBuffer result = ByteBuffer.allocateDirect(8);
    result.putLong(index);
    if (consumer != null && entry != null) {
      T value = serializer.readObject(entry);
      context.executor().execute(() -> consumer.accept(value));
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
      for (Iterator<Map.Entry<Long, LogSegment>> iterator = context.raft().log().segments().entrySet().iterator(); iterator.hasNext(); ) {
        Map.Entry<Long, LogSegment> entry = iterator.next();
        LogSegment segment = entry.getValue();
        if (context.raft().log().lastSegment() != segment
          && segment.lastIndex() != null
          && segment.lastIndex() <= commitIndex
          && !context.<EventLogConfig>config().getRetentionPolicy().retain(entry.getValue())) {
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
    return super.open()
      .thenRun(() -> {
        retentionFuture = context.scheduler().scheduleWithFixedDelay(this::compact, 0, context.<EventLogConfig>config()
          .getRetentionCheckInterval(), TimeUnit.MILLISECONDS);
      })
      .thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (retentionFuture != null) {
      retentionFuture.cancel(false);
    }
    return super.close();
  }

}
