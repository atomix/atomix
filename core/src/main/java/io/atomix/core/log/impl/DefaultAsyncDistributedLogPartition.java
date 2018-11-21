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

import io.atomix.core.log.AsyncDistributedLog;
import io.atomix.core.log.AsyncDistributedLogPartition;
import io.atomix.core.log.DistributedLogPartition;
import io.atomix.core.log.Record;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Default asynchronous distributed log partition implementation.
 */
public class DefaultAsyncDistributedLogPartition<E> implements AsyncDistributedLogPartition<E> {
  private final AsyncDistributedLog<E> log;
  private final LogSession session;
  private final Serializer serializer;

  public DefaultAsyncDistributedLogPartition(AsyncDistributedLog<E> log, LogSession session, Serializer serializer) {
    this.log = log;
    this.session = session;
    this.serializer = serializer;
  }

  @Override
  public int id() {
    return session.partitionId().id();
  }

  @Override
  public String name() {
    return log.name();
  }

  @Override
  public PrimitiveType type() {
    return log.type();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return log.protocol();
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

  /**
   * Produces the given bytes to the partition.
   *
   * @param bytes the bytes to produce
   * @return a future to be completed once the bytes have been written to the partition
   */
  CompletableFuture<Void> produce(byte[] bytes) {
    return session.producer().append(bytes).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> produce(E entry) {
    return produce(encode(entry));
  }

  @Override
  public CompletableFuture<Void> consume(long offset, Consumer<Record<E>> consumer) {
    return session.consumer().consume(offset, record ->
        consumer.accept(new Record<E>(record.index(), record.timestamp(), decode(record.value()))));
  }

  @Override
  public CompletableFuture<Void> close() {
    return session.close();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return Futures.exceptionalFuture(new UnsupportedOperationException("Cannot delete a single log partition"));
  }

  @Override
  public DistributedLogPartition<E> sync(Duration operationTimeout) {
    return new BlockingDistributedLogPartition<>(this, operationTimeout.toMillis());
  }
}
