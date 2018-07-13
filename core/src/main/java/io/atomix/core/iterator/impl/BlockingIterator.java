/*
 * Copyright 2018-present Open Networking Foundation
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
