// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.protocol.ProxyProtocol;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Repeatable reads transactional set.
 */
public class RepeatableReadsTransactionalSet<E> extends TransactionalSetParticipant<E> {
  private final Map<E, CompletableFuture<Boolean>> cache = Maps.newConcurrentMap();
  private final Map<E, SetUpdate<E>> updates = Maps.newConcurrentMap();

  public RepeatableReadsTransactionalSet(TransactionId transactionId, AsyncDistributedSet<E> set) {
    super(transactionId, set);
  }

  @Override
  public ProxyProtocol protocol() {
    return (ProxyProtocol) set.protocol();
  }

  private CompletableFuture<Boolean> read(E element) {
    return cache.computeIfAbsent(element, set::contains);
  }

  @Override
  public CompletableFuture<Boolean> add(E e) {
    SetUpdate<E> update = updates.get(e);
    if (update != null) {
      switch (update.type()) {
        case ADD:
          return CompletableFuture.completedFuture(false);
        case REMOVE:
        case NOT_CONTAINS:
          updates.put(e, new SetUpdate<>(SetUpdate.Type.ADD, e));
          return CompletableFuture.completedFuture(true);
        case CONTAINS:
          return CompletableFuture.completedFuture(false);
        default:
          throw new AssertionError();
      }
    } else {
      return read(e)
          .thenApply(exists -> {
            if (exists) {
              updates.put(e, new SetUpdate<>(SetUpdate.Type.CONTAINS, e));
              return false;
            } else {
              updates.put(e, new SetUpdate<>(SetUpdate.Type.ADD, e));
              return true;
            }
          });
    }
  }

  @Override
  public CompletableFuture<Boolean> remove(E e) {
    SetUpdate<E> update = updates.get(e);
    if (update != null) {
      switch (update.type()) {
        case ADD:
          updates.put(e, new SetUpdate<>(SetUpdate.Type.NOT_CONTAINS, e));
          return CompletableFuture.completedFuture(true);
        case REMOVE:
        case NOT_CONTAINS:
          return CompletableFuture.completedFuture(false);
        case CONTAINS:
          updates.put(e, new SetUpdate<>(SetUpdate.Type.REMOVE, e));
          return CompletableFuture.completedFuture(true);
        default:
          throw new AssertionError();
      }
    } else {
      return read(e)
          .thenApply(exists -> {
            if (exists) {
              updates.put(e, new SetUpdate<>(SetUpdate.Type.REMOVE, e));
              return true;
            } else {
              updates.put(e, new SetUpdate<>(SetUpdate.Type.NOT_CONTAINS, e));
              return false;
            }
          });
    }
  }

  @Override
  public CompletableFuture<Boolean> contains(E e) {
    SetUpdate<E> update = updates.get(e);
    if (update != null) {
      switch (update.type()) {
        case ADD:
        case CONTAINS:
          return CompletableFuture.completedFuture(true);
        case REMOVE:
        case NOT_CONTAINS:
          return CompletableFuture.completedFuture(false);
        default:
          throw new AssertionError();
      }
    }
    return read(e);
  }

  @Override
  public TransactionLog<SetUpdate<E>> log() {
    return new TransactionLog<>(transactionId, 0, Lists.newArrayList(updates.values()));
  }
}
