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

import io.atomix.core.map.ConsistentMapType;
import io.atomix.primitive.Consistency;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.Persistence;
import io.atomix.primitive.Replication;

/**
 * Transactional map builder.
 */
public abstract class TransactionalMapBuilder<K, V>
    extends DistributedPrimitiveBuilder<TransactionalMapBuilder<K, V>, TransactionalMap<K, V>> {
  protected TransactionalMapBuilder(String name) {
    super(ConsistentMapType.instance(), name);
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
}
