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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.ReferencePool;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Copycat event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventLog<K, V> extends AbstractResource<EventLog<K, V>> {

  /**
   * Creates a new event log with the given cluster and event log configurations.
   *
   * @param config The event log configuration.
   * @param cluster The cluster configuration.
   * @return A new event log instance.
   */
  public static <K, V> EventLog<K, V> create(EventLogConfig config, ClusterConfig cluster) {
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
  public static <K, V> EventLog<K, V> create(EventLogConfig config, ClusterConfig cluster, Executor executor) {
    return new EventLog<>(config, cluster, executor);
  }

  private final ReferencePool<Buffer> resultPool = new HeapBufferPool();
  private EventConsumer<K, V> consumer;

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
  public EventLog<K, V> consumer(EventConsumer<K, V> consumer) {
    this.consumer = consumer;
    return this;
  }

  /**
   * Commits an entry to the log.
   *
   * @param entry The entry key.
   * @return The entry to commit.
   */
  public CompletableFuture<Long> commit(V entry) {
    return commit(null, entry);
  }

  /**
   * Commits an entry to the log.
   *
   * @param key The entry key.
   * @param entry The entry to commit.
   * @return A completable future to be completed once the entry has been committed.
   */
  public CompletableFuture<Long> commit(K key, V entry) {
    return context.write(serializer.writeObject(key), serializer.writeObject(entry)).thenApplyAsync(Buffer::readLong, context.executor());
  }

  /**
   * Handles a log write.
   */
  private Buffer commit(long term, long index, Buffer key, Buffer entry) {
    Buffer result = resultPool.acquire();
    result.writeLong(index);
    if (consumer != null && entry != null) {
      context.executor().execute(() -> consumer.consume(serializer.readObject(key), serializer.readObject(entry)));
    }
    result.flip();
    return result;
  }

}
