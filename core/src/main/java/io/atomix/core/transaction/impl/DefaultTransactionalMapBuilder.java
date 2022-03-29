// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapBuilder;
import io.atomix.core.map.AtomicMapConfig;
import io.atomix.core.map.AtomicMapType;
import io.atomix.core.transaction.TransactionParticipant;
import io.atomix.core.transaction.TransactionalMap;
import io.atomix.core.transaction.TransactionalMapBuilder;
import io.atomix.core.transaction.TransactionalMapConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.serializer.Serializer;

/**
 * Transactional map builder.
 */
public class DefaultTransactionalMapBuilder<K, V> extends TransactionalMapBuilder<K, V> {
  private final AtomicMapBuilder<K, V> mapBuilder;
  private final DefaultTransaction transaction;

  public DefaultTransactionalMapBuilder(String name, TransactionalMapConfig config, PrimitiveManagementService managementService, DefaultTransaction transaction) {
    super(name, config, managementService);
    this.mapBuilder = AtomicMapType.<K, V>instance().newBuilder(name, new AtomicMapConfig(), managementService);
    this.transaction = transaction;
  }

  @Override
  public TransactionalMapBuilder<K, V> withProtocol(ProxyProtocol protocol) {
    mapBuilder.withProtocol(protocol);
    return this;
  }

  @Override
  public TransactionalMapBuilder<K, V> withSerializer(Serializer serializer) {
    mapBuilder.withSerializer(serializer);
    return this;
  }

  @Override
  public CompletableFuture<TransactionalMap<K, V>> getAsync() {
    return buildMap(mapBuilder::getAsync, SingletonTransactionalMap::new);
  }

  @Override
  public CompletableFuture<TransactionalMap<K, V>> buildAsync() {
    return buildMap(mapBuilder::buildAsync, m -> m);
  }

  private CompletableFuture<TransactionalMap<K, V>> buildMap(
      Supplier<CompletableFuture<AtomicMap<K, V>>> mapSupplier,
      Function<TransactionalMapParticipant<K, V>, TransactionParticipant<?>> mapWrapper) {
    return mapSupplier.get()
        .thenApply(map -> {
          TransactionalMapParticipant<K, V> transactionalMap;
          switch (transaction.isolation()) {
            case READ_COMMITTED:
              transactionalMap = new ReadCommittedTransactionalMap<>(transaction.transactionId(), map.async());
              break;
            case REPEATABLE_READS:
              transactionalMap = new RepeatableReadsTransactionalMap<>(transaction.transactionId(), map.async());
              break;
            default:
              throw new AssertionError();
          }
          transaction.addParticipants(mapWrapper.apply(transactionalMap));
          return transactionalMap.sync();
        });
  }
}
