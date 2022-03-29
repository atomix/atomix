// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.transaction.TransactionParticipant;

/**
 * Singleton transactional set.
 */
public class SingletonTransactionalSet<E> extends DelegatingTransactionalSet<E> implements TransactionParticipant<SetUpdate<E>> {
  private final TransactionalSetParticipant<E> set;

  public SingletonTransactionalSet(TransactionalSetParticipant<E> set) {
    super(set);
    this.set = set;
  }

  @Override
  public TransactionLog<SetUpdate<E>> log() {
    return set.log();
  }

  @Override
  public CompletableFuture<Boolean> prepare() {
    return set.prepare();
  }

  @Override
  public CompletableFuture<Void> commit() {
    return set.commit();
  }

  @Override
  public CompletableFuture<Void> rollback() {
    return set.rollback();
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return CompletableFuture.completedFuture(null);
  }
}
