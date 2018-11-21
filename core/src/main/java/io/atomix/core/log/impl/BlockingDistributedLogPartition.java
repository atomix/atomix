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

import com.google.common.base.Throwables;
import io.atomix.core.log.AsyncDistributedLogPartition;
import io.atomix.core.log.DistributedLogPartition;
import io.atomix.core.log.Record;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Blocking distributed log partition.
 */
public class BlockingDistributedLogPartition<E> extends Synchronous<AsyncDistributedLogPartition<E>> implements DistributedLogPartition<E> {
  private final AsyncDistributedLogPartition<E> asyncPartition;
  private final long operationTimeoutMillis;

  public BlockingDistributedLogPartition(AsyncDistributedLogPartition<E> asyncPartition, long operationTimeoutMillis) {
    super(asyncPartition);
    this.asyncPartition = asyncPartition;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public int id() {
    return asyncPartition.id();
  }

  @Override
  public void produce(E entry) {
    complete(asyncPartition.produce(entry));
  }

  @Override
  public void consume(long offset, Consumer<Record<E>> consumer) {
    complete(asyncPartition.consume(offset, consumer));
  }

  @Override
  public AsyncDistributedLogPartition<E> async() {
    return asyncPartition;
  }

  private <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      Throwable cause = Throwables.getRootCause(e);
      if (cause instanceof PrimitiveException) {
        throw (PrimitiveException) cause;
      } else {
        throw new PrimitiveException(cause);
      }
    }
  }
}
