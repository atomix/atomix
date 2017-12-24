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

import io.atomix.primitive.Consistency;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.Persistence;
import io.atomix.primitive.Replication;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transaction builder.
 */
public abstract class TransactionBuilder extends DistributedPrimitiveBuilder<TransactionBuilder, Transaction> {
  private Isolation isolation = Isolation.READ_COMMITTED;

  protected TransactionBuilder(String name) {
    super(TransactionType.instance(), name);
  }

  @Override
  protected Consistency defaultConsistency() {
    return Consistency.LINEARIZABLE;
  }

  @Override
  protected Persistence defaultPersistence() {
    return Persistence.PERSISTENT;
  }

  @Override
  protected Replication defaultReplication() {
    return Replication.SYNCHRONOUS;
  }

  /**
   * Sets the transaction isolation level.
   *
   * @param isolation the transaction isolation level
   * @return the transaction builder
   */
  public TransactionBuilder withIsolation(Isolation isolation) {
    this.isolation = checkNotNull(isolation, "isolation cannot be null");
    return this;
  }

  /**
   * Returns the transaction isolation level.
   *
   * @return the transaction isolation level
   */
  public Isolation isolation() {
    return isolation;
  }
}
