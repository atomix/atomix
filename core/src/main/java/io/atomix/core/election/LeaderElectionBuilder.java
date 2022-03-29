// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.election;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Builder for constructing new {@link AsyncLeaderElection} instances.
 */
public abstract class LeaderElectionBuilder<T>
    extends PrimitiveBuilder<LeaderElectionBuilder<T>, LeaderElectionConfig, LeaderElection<T>>
    implements ProxyCompatibleBuilder<LeaderElectionBuilder<T>> {

  protected LeaderElectionBuilder(String name, LeaderElectionConfig config, PrimitiveManagementService managementService) {
    super(LeaderElectionType.instance(), name, config, managementService);
  }

  @Override
  public LeaderElectionBuilder<T> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
