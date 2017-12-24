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
import io.atomix.core.map.ConsistentMapType;
import io.atomix.core.transaction.TransactionalMap;
import io.atomix.core.transaction.TransactionalMapBuilder;
import io.atomix.primitive.PrimitiveManagementService;

import java.util.concurrent.CompletableFuture;

/**
 * Transactional map builder.
 */
public class DefaultTransactionalMapBuilder<K, V> extends TransactionalMapBuilder<K, V> {
  private final ConsistentMapBuilder<K, V> mapBuilder;
  private final DefaultTransaction transaction;

  public DefaultTransactionalMapBuilder(String name, PrimitiveManagementService managementService, DefaultTransaction transaction) {
    super(name);
    this.mapBuilder = ConsistentMapType.<K, V>instance().newPrimitiveBuilder(name, managementService);
    this.transaction = transaction;
  }

  @Override
  public CompletableFuture<TransactionalMap<K, V>> buildAsync() {
    return mapBuilder.buildAsync()
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
          transaction.addParticipants(transactionalMap);
          return transactionalMap.sync();
        });
  }
}
