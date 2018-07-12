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
package io.atomix.core.list;

import io.atomix.core.collection.DistributedCollectionBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Builder for distributed queue.
 *
 * @param <E> queue element type
 */
public abstract class DistributedListBuilder<E>
    extends DistributedCollectionBuilder<DistributedListBuilder<E>, DistributedListConfig, DistributedList<E>, E>
    implements ProxyCompatibleBuilder<DistributedListBuilder<E>> {

  protected DistributedListBuilder(String name, DistributedListConfig config, PrimitiveManagementService managementService) {
    super(DistributedListType.instance(), name, config, managementService);
  }

  @Override
  public DistributedListBuilder<E> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
