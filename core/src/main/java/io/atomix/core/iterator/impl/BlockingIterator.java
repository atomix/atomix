// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.iterator.impl;

import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.SyncIterator;
import io.atomix.primitive.PrimitiveException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Blocking iterator.
 */
public class BlockingIterator<T> implements SyncIterator<T> {
  private final AsyncIterator<T> asyncIterator;
  private final long operationTimeoutMillis;

  public BlockingIterator(AsyncIterator<T> asyncIterator, long operationTimeoutMillis) {
    this.asyncIterator = asyncIterator;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public boolean hasNext() {
    return complete(asyncIterator.hasNext());
  }

  @Override
  public T next() {
    return complete(asyncIterator.next());
  }

  @Override
  public void close() {
    complete(asyncIterator.close());
  }

  @Override
  public AsyncIterator<T> async() {
    return asyncIterator;
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
      if (e.getCause() instanceof PrimitiveException) {
        throw (PrimitiveException) e.getCause();
      } else {
        throw new PrimitiveException(e.getCause());
      }
    }
  }
}
