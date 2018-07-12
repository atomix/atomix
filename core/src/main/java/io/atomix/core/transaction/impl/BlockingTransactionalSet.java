/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.transaction.impl;

import com.google.common.base.Throwables;
import io.atomix.core.transaction.AsyncTransactionalSet;
import io.atomix.core.transaction.TransactionalSet;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Blocking transactional map.
 */
public class BlockingTransactionalSet<E> implements TransactionalSet<E> {
  private final AsyncTransactionalSet<E> asyncSet;
  private final long operationTimeoutMillis;

  public BlockingTransactionalSet(AsyncTransactionalSet<E> asyncSet, long operationTimeoutMillis) {
    this.asyncSet = asyncSet;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public String name() {
    return asyncSet.name();
  }

  @Override
  public PrimitiveType type() {
    return asyncSet.type();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return asyncSet.protocol();
  }

  @Override
  public boolean add(E element) {
    return complete(asyncSet.add(element));
  }

  @Override
  public boolean remove(E element) {
    return complete(asyncSet.remove(element));
  }

  @Override
  public boolean contains(E element) {
    return complete(asyncSet.contains(element));
  }

  @Override
  public void close() {
    complete(asyncSet.close());
  }

  @Override
  public AsyncTransactionalSet<E> async() {
    return asyncSet;
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
      Throwables.propagateIfPossible(e.getCause());
      throw new PrimitiveException(e.getCause());
    }
  }
}
