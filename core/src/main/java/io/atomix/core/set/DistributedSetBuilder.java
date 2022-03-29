// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
