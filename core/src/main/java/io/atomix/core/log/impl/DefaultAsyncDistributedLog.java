/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.log.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.google.common.io.BaseEncoding;
import io.atomix.core.log.AsyncDistributedLog;
import io.atomix.core.log.DistributedLog;
import io.atomix.core.log.DistributedLogType;
import io.atomix.core.log.Record;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.log.LogClient;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Serializer;

/**
 * Default distributed log.
 */
public class DefaultAsyncDistributedLog<E> implements AsyncDistributedLog<E> {
  private final LogClient client;
  private final Serializer serializer;

  public DefaultAsyncDistributedLog(LogClient client, Serializer serializer) {
    this.client = client;
    this.serializer = serializer;
  }

  @Override
  public String name() {
    return null;
  }

  @Override
  public PrimitiveType type() {
    return DistributedLogType.instance();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return client.protocol();
  }

  /**
   * Encodes the given object using the configured {@link #serializer}.
   *
   * @param object the object to encode
   * @param <T>    the object type
   * @return the encoded bytes
   */
  private <T> byte[] encode(T object) {
    return object != null ? serializer.encode(object) : null;
  }

  /**
   * Decodes the given object using the configured {@link #serializer}.
   *
   * @param bytes the bytes to decode
   * @param <T>   the object type
   * @return the decoded object
   */
  private <T> T decode(byte[] bytes) {
    return bytes != null ? serializer.decode(bytes) : null;
  }

  @Override
  public CompletableFuture<Void> produce(E entry) {
    byte[] bytes = encode(entry);
    return client.getPartition(BaseEncoding.base16().encode(bytes)).producer().append(bytes).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> consume(long offset, Consumer<Record<E>> consumer) {
    return Futures.allOf(client.getPartitions().stream()
        .map(partition -> partition.consumer().consume(offset, record ->
            consumer.accept(new Record<E>(record.index(), record.timestamp(), decode(record.value()))))))
        .thenApply(v -> null);
  }

  @Override
  public DistributedLog<E> sync(Duration operationTimeout) {
    return new BlockingDistributedLog<>(this, operationTimeout.toMillis());
  }

  @Override
  public CompletableFuture<Void> close() {
    return client.close();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return client.delete();
  }
}
