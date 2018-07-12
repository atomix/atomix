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
package io.atomix.core.transaction;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;

/**
 * Transaction builder.
 */
public abstract class TransactionBuilder extends PrimitiveBuilder<TransactionBuilder, TransactionConfig, Transaction> {
  protected TransactionBuilder(String name, TransactionConfig config, PrimitiveManagementService managementService) {
    super(TransactionType.instance(), name, config, managementService);
  }

  /**
   * Sets the transaction isolation level.
   *
   * @param isolation the transaction isolation level
   * @return the transaction builder
   */
  public TransactionBuilder withIsolation(Isolation isolation) {
    config.setIsolation(isolation);
    return this;
  }
}
