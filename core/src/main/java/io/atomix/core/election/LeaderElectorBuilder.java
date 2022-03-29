// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.election;

import io.atomix.core.cache.CachedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Builder for constructing new {@link AsyncLeaderElector} instances.
 */
public abstract class LeaderElectorBuilder<T>
    extends CachedPrimitiveBuilder<LeaderElectorBuilder<T>, LeaderElectorConfig, LeaderElector<T>>
    implements ProxyCompatibleBuilder<LeaderElectorBuilder<T>> {

  protected LeaderElectorBuilder(String name, LeaderElectorConfig config, PrimitiveManagementService managementService) {
    super(LeaderElectorType.instance(), name, config, managementService);
  }

  @Override
  public LeaderElectorBuilder<T> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
