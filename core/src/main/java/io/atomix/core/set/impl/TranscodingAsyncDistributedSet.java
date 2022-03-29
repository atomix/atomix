// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.core.set.impl;

import io.atomix.core.collection.impl.TranscodingAsyncDistributedCollection;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An {@code AsyncDistributedSet} that maps its operations to operations on a
 * differently typed {@code AsyncDistributedSet} by transcoding operation inputs and outputs.
 *
 * @param <E2> key type of other map
 * @param <E1> key type of this map
 */
public class TranscodingAsyncDistributedSet<E1, E2> extends TranscodingAsyncDistributedCollection<E1, E2> implements AsyncDistributedSet<E1> {
  private final AsyncDistributedSet<E2> backingSet;
  protected final Function<E1, E2> entryEncoder;
  protected final Function<E2, E1> entryDecoder;

  public TranscodingAsyncDistributedSet(
      AsyncDistributedSet<E2> backingSet,
      Function<E1, E2> entryEncoder,
      Function<E2, E1> entryDecoder) {
    super(backingSet, entryEncoder, entryDecoder);
    this.backingSet = backingSet;
    this.entryEncoder = e -> e == null ? null : entryEncoder.apply(e);
    this.entryDecoder = e -> e == null ? null : entryDecoder.apply(e);
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<E1>> transactionLog) {
    return backingSet.prepare(transactionLog.map(record -> record.map(entryEncoder)));
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return backingSet.commit(transactionId);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return backingSet.rollback(transactionId);
  }

  @Override
  public DistributedSet<E1> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }
}
