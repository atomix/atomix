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

import io.atomix.core.transaction.AsyncTransaction;
import io.atomix.core.transaction.CommitStatus;
import io.atomix.core.transaction.Isolation;
import io.atomix.core.transaction.Transaction;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionalMapBuilder;
import io.atomix.core.transaction.TransactionalSetBuilder;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Blocking transaction.
 */
public class BlockingTransaction implements Transaction {
  private final AsyncTransaction asyncTransaction;
  private final long operationTimeoutMillis;

  public BlockingTransaction(AsyncTransaction asyncTransaction, long operationTimeoutMillis) {
    this.asyncTransaction = checkNotNull(asyncTransaction);
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public String name() {
    return asyncTransaction.name();
  }

  @Override
  public PrimitiveType type() {
    return asyncTransaction.type();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return asyncTransaction.protocol();
  }

  @Override
  public TransactionId transactionId() {
    return asyncTransaction.transactionId();
  }

  @Override
  public Isolation isolation() {
    return asyncTransaction.isolation();
  }

  @Override
  public boolean isOpen() {
    return asyncTransaction.isOpen();
  }

  @Override
  public void begin() {
    complete(asyncTransaction.begin());
  }

  @Override
  public CommitStatus commit() {
    return complete(asyncTransaction.commit());
  }

  @Override
  public void abort() {
    complete(asyncTransaction.abort());
  }

  @Override
  public <K, V> TransactionalMapBuilder<K, V> mapBuilder(String name) {
    return asyncTransaction.mapBuilder(name);
  }

  @Override
  public <E> TransactionalSetBuilder<E> setBuilder(String name) {
    return asyncTransaction.setBuilder(name);
  }

  @Override
  public void close() {
    complete(asyncTransaction.close());
  }

  @Override
  public AsyncTransaction async() {
    return asyncTransaction;
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
