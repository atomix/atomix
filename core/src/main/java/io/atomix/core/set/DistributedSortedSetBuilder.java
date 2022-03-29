// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
