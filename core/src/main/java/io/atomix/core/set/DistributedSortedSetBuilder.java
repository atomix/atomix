/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.set;

import io.atomix.core.collection.DistributedCollectionBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.protocol.set.SortedSetCompatibleBuilder;
import io.atomix.primitive.protocol.set.SortedSetProtocol;

/**
 * Builder for distributed sorted set.
 *
 * @param <E> type set elements.
 */
public abstract class DistributedSortedSetBuilder<E extends Comparable<E>>
    extends DistributedCollectionBuilder<DistributedSortedSetBuilder<E>, DistributedSortedSetConfig, DistributedSortedSet<E>, E>
    implements ProxyCompatibleBuilder<DistributedSortedSetBuilder<E>>,
    SortedSetCompatibleBuilder<DistributedSortedSetBuilder<E>> {

  protected DistributedSortedSetBuilder(String name, DistributedSortedSetConfig config, PrimitiveManagementService managementService) {
    super(DistributedSortedSetType.instance(), name, config, managementService);
  }

  @Override
  public DistributedSortedSetBuilder<E> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  @Override
  public DistributedSortedSetBuilder<E> withProtocol(SortedSetProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
