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
package io.atomix.cluster;

import io.atomix.utils.ConfiguredType;
import io.atomix.utils.config.Configured;
import io.atomix.utils.config.TypedConfig;
import io.atomix.utils.event.ListenerService;
import io.atomix.utils.net.Address;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Cluster membership provider.
 * <p>
 * The member location provider is an SPI that the {@link ClusterMembershipService} uses to locate new members joining
 * the cluster. It provides a simple TCP {@link Address} for members which will be used by the
 * {@link ClusterMembershipService} to exchange higher level {@link Member} information. The location provider is only
 * used for member discovery, not failure detection.
 */
public interface ClusterMembershipProvider
    extends ListenerService<MemberLocationEvent, MemberLocationEventListener>, Configured<ClusterMembershipProvider.Config> {

  /**
   * Membership provider type.
   */
  interface Type<C extends Config> extends ConfiguredType<C> {

    /**
     * Creates a new instance of the provider.
     *
     * @param config the provider configuration
     * @return the provider instance
     */
    ClusterMembershipProvider newProvider(C config);
  }

  /**
   * Membership provider configuration.
   */
  interface Config extends TypedConfig<Type> {
  }

  /**
   * Membership provider builder.
   */
  interface Builder extends io.atomix.utils.Builder<ClusterMembershipProvider> {
  }

  /**
   * Returns the set of active member locations.
   *
   * @return the set of active member locations
   */
  Set<Address> getLocations();

  /**
   * Joins the cluster.
   *
   * @param bootstrap the bootstrap service
   * @param address   the address with which to join
   * @return a future to be completed once the join is complete
   */
  CompletableFuture<Void> join(BootstrapService bootstrap, Address address);

  /**
   * Leaves the cluster.
   *
   * @param address the address with which to leave
   * @return a future to be completed once the leave is complete
   */
  CompletableFuture<Void> leave(Address address);

}
