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

import io.atomix.utils.event.ListenerService;
import io.atomix.utils.net.Address;

import java.util.concurrent.CompletableFuture;

/**
 * Cluster member location provider.
 * <p>
 * The member location provider is an SPI that the {@link ClusterMembershipService} uses to locate new members joining
 * the cluster. It provides a simple TCP {@link Address} for members which will be used by the
 * {@link ClusterMembershipService} to exchange higher level {@link Member} information. The location provider is only
 * used for member discovery, not failure detection.
 */
public interface MemberLocationProvider extends ListenerService<MemberLocationEvent, MemberLocationEventListener> {

  /**
   * Joins the cluster.
   *
   * @param address the address with which to join
   * @return a future to be completed once the join is complete
   */
  CompletableFuture<Void> join(Address address);

  /**
   * Leaves the cluster.
   *
   * @return a future to be completed once the leave is complete
   */
  CompletableFuture<Void> leave();

}
