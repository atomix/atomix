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

import io.atomix.core.map.ConsistentMapBuilder;
import io.atomix.core.map.ConsistentMapConfig;
import io.atomix.core.map.ConsistentMapType;
import io.atomix.core.transaction.TransactionalSet;
import io.atomix.core.transaction.TransactionalSetBuilder;
import io.atomix.core.transaction.TransactionalSetConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Default transactional set builder.
 */
public class DefaultTransactionalSetBuilder<E> extends TransactionalSetBuilder<E> {
  private final DefaultTransaction transaction;
  private final ConsistentMapBuilder<E, Boolean> mapBuilder;

  public DefaultTransactionalSetBuilder(String name, TransactionalSetConfig config, PrimitiveManagementService managementService, DefaultTransaction transaction) {
    super(name, config, managementService);
    this.transaction = transaction;
    this.mapBuilder = ConsistentMapType.<E, Boolean>instance().newBuilder(name, new ConsistentMapConfig(), managementService);
  }

  @Override
  public TransactionalSetBuilder<E> withSerializer(Serializer serializer) {
    mapBuilder.withSerializer(serializer);
    return this;
  }

  @Override
  public TransactionalSetBuilder<E> withProtocol(PrimitiveProtocol protocol) {
    mapBuilder.withProtocol(protocol);
    return this;
  }

  @Override
  public CompletableFuture<TransactionalSet<E>> buildAsync() {
    return mapBuilder.buildAsync()
        .thenApply(map -> {
          TransactionalMapParticipant<E, Boolean> transactionalMap;
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
          transaction.addParticipants(transactionalMap);
          return transactionalMap;
        }).thenApply(map -> new DefaultTransactionalSet<>(map).sync());
  }
}
