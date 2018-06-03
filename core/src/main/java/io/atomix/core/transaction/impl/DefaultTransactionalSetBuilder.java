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

import io.atomix.core.transaction.TransactionalMapConfig;
import io.atomix.core.transaction.TransactionalSet;
import io.atomix.core.transaction.TransactionalSetBuilder;
import io.atomix.core.transaction.TransactionalSetConfig;
import io.atomix.primitive.PrimitiveManagementService;

import java.util.concurrent.CompletableFuture;

/**
 * Default transactional set builder.
 */
public class DefaultTransactionalSetBuilder<E> extends TransactionalSetBuilder<E> {
  private final DefaultTransaction transaction;

  public DefaultTransactionalSetBuilder(String name, TransactionalSetConfig config, PrimitiveManagementService managementService, DefaultTransaction transaction) {
    super(name, config, managementService);
    this.transaction = transaction;
  }

  @Override
  public CompletableFuture<TransactionalSet<E>> buildAsync() {
    return new DefaultTransactionalMapBuilder<E, Boolean>(name(), new TransactionalMapConfig(), managementService, transaction)
        .buildAsync()
        .thenApply(map -> new DefaultTransactionalSet<>(map.async()).sync());
  }
}
