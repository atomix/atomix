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
import io.atomix.primitive.protocol.set.SetCompatibleBuilder;
import io.atomix.primitive.protocol.set.SetProtocol;

/**
 * Builder for distributed set.
 *
 * @param <E> type set elements.
 */
public abstract class DistributedSetBuilder<E>
    extends DistributedCollectionBuilder<DistributedSetBuilder<E>, DistributedSetConfig, DistributedSet<E>, E>
    implements ProxyCompatibleBuilder<DistributedSetBuilder<E>>, SetCompatibleBuilder<DistributedSetBuilder<E>> {

  protected DistributedSetBuilder(String name, DistributedSetConfig config, PrimitiveManagementService managementService) {
    super(DistributedSetType.instance(), name, config, managementService);
  }

  @Override
  public DistributedSetBuilder<E> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  @Override
  public DistributedSetBuilder<E> withProtocol(SetProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
