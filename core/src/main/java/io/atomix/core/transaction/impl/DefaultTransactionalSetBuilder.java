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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetBuilder;
import io.atomix.core.set.DistributedSetConfig;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.transaction.TransactionParticipant;
import io.atomix.core.transaction.TransactionalSet;
import io.atomix.core.transaction.TransactionalSetBuilder;
import io.atomix.core.transaction.TransactionalSetConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.serializer.Serializer;

/**
 * Default transactional set builder.
 */
public class DefaultTransactionalSetBuilder<E> extends TransactionalSetBuilder<E> {
  private final DistributedSetBuilder<E> setBuilder;
  private final DefaultTransaction transaction;

  public DefaultTransactionalSetBuilder(String name, TransactionalSetConfig config, PrimitiveManagementService managementService, DefaultTransaction transaction) {
    super(name, config, managementService);
    this.setBuilder = DistributedSetType.<E>instance().newBuilder(name, new DistributedSetConfig(), managementService);
    this.transaction = transaction;
  }

  @Override
  public TransactionalSetBuilder<E> withProtocol(ProxyProtocol protocol) {
    setBuilder.withProtocol(protocol);
    return this;
  }

  @Override
  public TransactionalSetBuilder<E> withSerializer(Serializer serializer) {
    setBuilder.withSerializer(serializer);
    return this;
  }

  @Override
  public CompletableFuture<TransactionalSet<E>> getAsync() {
    return buildSet(setBuilder::getAsync, SingletonTransactionalSet::new);
  }

  @Override
  public CompletableFuture<TransactionalSet<E>> buildAsync() {
    return buildSet(setBuilder::buildAsync, s -> s);
  }

  private CompletableFuture<TransactionalSet<E>> buildSet(
      Supplier<CompletableFuture<DistributedSet<E>>> setSupplier,
      Function<TransactionalSetParticipant<E>, TransactionParticipant<?>> setWrapper) {
    return setSupplier.get()
        .thenApply(set -> {
          TransactionalSetParticipant<E> transactionalSet;
          switch (transaction.isolation()) {
            case READ_COMMITTED:
              transactionalSet = new ReadCommittedTransactionalSet<>(transaction.transactionId(), set.async());
              break;
            case REPEATABLE_READS:
              transactionalSet = new RepeatableReadsTransactionalSet<>(transaction.transactionId(), set.async());
              break;
            default:
              throw new AssertionError();
          }
          transaction.addParticipants(setWrapper.apply(transactionalSet));
          return transactionalSet.sync();
        });
  }
}
