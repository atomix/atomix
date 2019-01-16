package io.atomix.core.transaction.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.core.map.impl.MapUpdate;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.transaction.TransactionParticipant;

/**
 * Singleton transactional map.
 */
public class SingletonTransactionalMap<K, V> extends DelegatingTransactionalMap<K, V> implements TransactionParticipant<MapUpdate<K, V>> {
  private final TransactionalMapParticipant<K, V> map;

  public SingletonTransactionalMap(TransactionalMapParticipant<K, V> map) {
    super(map);
    this.map = map;
  }

  @Override
  public TransactionLog<MapUpdate<K, V>> log() {
    return map.log();
  }

  @Override
  public CompletableFuture<Boolean> prepare() {
    return map.prepare();
  }

  @Override
  public CompletableFuture<Void> commit() {
    return map.commit();
  }

  @Override
  public CompletableFuture<Void> rollback() {
    return map.rollback();
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
