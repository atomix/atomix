// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
