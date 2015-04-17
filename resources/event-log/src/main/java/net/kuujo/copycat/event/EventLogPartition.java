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

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.ReferencePool;
import net.kuujo.copycat.resource.PartitionContext;
import net.kuujo.copycat.resource.internal.AbstractPartition;

import java.util.concurrent.CompletableFuture;

/**
 * Copycat event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventLogPartition<K, V> extends AbstractPartition<EventLogPartition<K, V>> {
  private EventConsumer<K, V> consumer;
  private final ReferencePool<Buffer> bufferPool = new HeapBufferPool();

  public EventLogPartition(PartitionContext context) {
    super(context);
  }

  /**
   * Registers a log entry consumer.
   *
   * @param consumer The log entry consumer.
   * @return The event log.
   */
  public EventLogPartition<K, V> consumer(EventConsumer<K, V> consumer) {
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
    Buffer keyBuffer = key != null ? serializer.writeObject(key, bufferPool.acquire()).flip() : null;
    Buffer entryBuffer = bufferPool.acquire();
    serializer.writeObject(entry, entryBuffer);
    entryBuffer.flip();

    return context.write(keyBuffer, entryBuffer)
      .thenApplyAsync(result -> {
        if (keyBuffer != null)
          keyBuffer.close();
        entryBuffer.close();
        return result.readLong();
      },context.getExecutor());
  }

  /**
   * Handles a log write.
   */
  private Buffer commit(Buffer key, Buffer entry, Buffer result) {
    if (consumer != null && entry != null) {
      context.getExecutor().execute(() -> consumer.consume(serializer.readObject(key), serializer.readObject(entry)));
    }
    return result.flip();
  }

}
