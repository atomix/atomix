// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set;

import io.atomix.core.collection.DistributedCollectionBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.protocol.set.NavigableSetCompatibleBuilder;
import io.atomix.primitive.protocol.set.NavigableSetProtocol;

/**
 * Builder for distributed navigable set.
 *
 * @param <E> type set elements.
 */
public abstract class DistributedNavigableSetBuilder<E extends Comparable<E>>
    extends DistributedCollectionBuilder<DistributedNavigableSetBuilder<E>, DistributedNavigableSetConfig, DistributedNavigableSet<E>, E>
    implements ProxyCompatibleBuilder<DistributedNavigableSetBuilder<E>>, NavigableSetCompatibleBuilder<DistributedNavigableSetBuilder<E>> {

  protected DistributedNavigableSetBuilder(String name, DistributedNavigableSetConfig config, PrimitiveManagementService managementService) {
    super(DistributedNavigableSetType.instance(), name, config, managementService);
  }

  @Override
  public DistributedNavigableSetBuilder<E> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  @Override
  public DistributedNavigableSetBuilder<E> withProtocol(NavigableSetProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
