/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.barrier.impl;

import com.google.common.base.Throwables;
import io.atomix.core.barrier.AsyncDistributedCyclicBarrier;
import io.atomix.core.barrier.DistributedCyclicBarrier;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default implementation for a {@code DistributedCyclicBarrier} backed by a {@link AsyncDistributedCyclicBarrier}.
 */
public class BlockingDistributedCyclicBarrier extends Synchronous<AsyncDistributedCyclicBarrier> implements DistributedCyclicBarrier {

  private final AsyncDistributedCyclicBarrier asyncBarrier;
  private final long operationTimeoutMillis;

  public BlockingDistributedCyclicBarrier(AsyncDistributedCyclicBarrier asyncBarrier, long operationTimeoutMillis) {
    super(asyncBarrier);
    this.asyncBarrier = asyncBarrier;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public int await() {
    return complete(asyncBarrier.await());
  }

  @Override
  public int await(Duration timeout) {
    return complete(asyncBarrier.await(timeout));
  }

  @Override
  public int getNumberWaiting() {
    return complete(asyncBarrier.getNumberWaiting());
  }

  @Override
  public int getParties() {
    return complete(asyncBarrier.getParties());
  }

  @Override
  public boolean isBroken() {
    return complete(asyncBarrier.isBroken());
  }

  @Override
  public void reset() {
    complete(asyncBarrier.reset());
  }

  @Override
  public AsyncDistributedCyclicBarrier async() {
    return asyncBarrier;
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
