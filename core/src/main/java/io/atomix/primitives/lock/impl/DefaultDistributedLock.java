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
package io.atomix.primitives.lock.impl;

import io.atomix.primitives.PrimitiveException;
import io.atomix.primitives.Synchronous;
import io.atomix.primitives.lock.AsyncDistributedLock;
import io.atomix.primitives.lock.DistributedLock;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default implementation for a {@code DistributedLock} backed by a {@link AsyncDistributedLock}.
 */
public class DefaultDistributedLock extends Synchronous<AsyncDistributedLock> implements DistributedLock {

  private final AsyncDistributedLock asyncLock;
  private final long operationTimeoutMillis;

  public DefaultDistributedLock(AsyncDistributedLock asyncLock, long operationTimeoutMillis) {
    super(asyncLock);
    this.asyncLock = asyncLock;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public void lock() {
    complete(asyncLock.lock());
  }

  @Override
  public boolean tryLock() {
    return complete(asyncLock.tryLock());
  }

  @Override
  public boolean tryLock(Duration timeout) {
    return complete(asyncLock.tryLock(timeout));
  }

  @Override
  public void unlock() {
    complete(asyncLock.unlock());
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
      throw new PrimitiveException(e.getCause());
    }
  }
}
