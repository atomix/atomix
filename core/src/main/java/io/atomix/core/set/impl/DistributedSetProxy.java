// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.core.collection.impl.PartitionedDistributedCollectionProxy;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.impl.PrepareResult;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Distributed set proxy.
 */
public class DistributedSetProxy extends PartitionedDistributedCollectionProxy<AsyncDistributedSet<String>, DistributedSetService<String>>
    implements AsyncDistributedSet<String> {

  public DistributedSetProxy(ProxyClient<DistributedSetService<String>> client, PrimitiveRegistry registry) {
    super(client, registry);
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<String>> transactionLog) {
    Map<PartitionId, List<SetUpdate<String>>> updatesGroupedBySet = Maps.newIdentityHashMap();
    transactionLog.records().forEach(update -> {
      updatesGroupedBySet.computeIfAbsent(getProxyClient().getPartitionId(update.element()), k -> Lists.newLinkedList()).add(update);
    });
    Map<PartitionId, TransactionLog<SetUpdate<String>>> transactionsBySet =
        Maps.transformValues(updatesGroupedBySet, list -> new TransactionLog<>(transactionLog.transactionId(), transactionLog.version(), list));

    return Futures.allOf(transactionsBySet.entrySet()
        .stream()
        .map(e -> getProxyClient().applyOn(e.getKey(), service -> service.prepare(e.getValue()))
            .thenApply(v -> v == PrepareResult.OK || v == PrepareResult.PARTIAL_FAILURE))
        .collect(Collectors.toList()))
        .thenApply(list -> list.stream().reduce(Boolean::logicalAnd).orElse(true));
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return getProxyClient().applyAll(service -> service.commit(transactionId))
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return getProxyClient().applyAll(service -> service.rollback(transactionId))
        .thenApply(v -> null);
  }

  @Override
  public DistributedSet<String> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }
}
