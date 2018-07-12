/*
 * Copyright 2018-present Open Networking Foundation
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

import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveType;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transaction configuration.
 */
public class TransactionConfig extends PrimitiveConfig<TransactionConfig> {
  private Isolation isolation = Isolation.READ_COMMITTED;

  @Override
  public PrimitiveType getType() {
    return TransactionType.instance();
  }

  /**
   * Sets the transaction isolation level.
   *
   * @param isolation the transaction isolation level
   * @return the transaction configuration
   */
  public TransactionConfig setIsolation(Isolation isolation) {
    this.isolation = checkNotNull(isolation, "isolation cannot be null");
    return this;
  }

  /**
   * Returns the transaction isolation level.
   *
   * @return the transaction isolation level
   */
  public Isolation getIsolation() {
    return isolation;
  }
}
