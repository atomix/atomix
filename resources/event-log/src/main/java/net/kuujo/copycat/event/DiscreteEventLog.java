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
package net.kuujo.copycat.event;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.ReferencePool;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.protocol.Persistence;
import net.kuujo.copycat.resource.DiscreteResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Discrete state log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DiscreteEventLog<K, V> extends DiscreteResource<DiscreteEventLog<K, V>, EventLog<K, V>> implements EventLog<K, V> {

  /**
   * Returns a new state log builder.
   *
   * @return A new state log builder.
   */
  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DiscreteEventLog.class);
  private final ReferencePool<Buffer> bufferPool = new HeapBufferPool();
  private EventConsumer<K, V> consumer;

  public DiscreteEventLog(EventLogConfig config) {
    super(config);
  }

  @Override
  protected Buffer commit(Buffer key, Buffer entry, Buffer result) {
    if (consumer != null)
      consumer.consume(serializer.readObject(key), serializer.readObject(entry));
    return result;
  }

  @Override
  public EventLog<K, V> consumer(EventConsumer<K, V> consumer) {
    this.consumer = consumer;
    return this;
  }

  @Override
  public CompletableFuture<Long> commit(K key, V value) {
    Buffer keyBuffer = key != null ? serializer.writeObject(key, bufferPool.acquire()).flip() : null;
    Buffer entryBuffer = bufferPool.acquire();
    serializer.writeObject(value, entryBuffer);
    entryBuffer.flip();

    return protocol.submit(keyBuffer, entryBuffer, Persistence.DURABLE, Consistency.LEASE).thenApply(result -> {
      if (keyBuffer != null)
        keyBuffer.close();
      entryBuffer.close();
      return result.readLong();
    });
  }

  /**
   * State log builder.
   */
  public static class Builder<K, V> extends DiscreteResource.Builder<Builder<K, V>, DiscreteEventLog<K, V>> {
    private final EventLogConfig config = new EventLogConfig();

    private Builder() {
      this(new EventLogConfig());
    }

    private Builder(EventLogConfig config) {
      super(config);
    }

    @Override
    public DiscreteEventLog<K, V> build() {
      return new DiscreteEventLog<>(config);
    }
  }

}
