// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
