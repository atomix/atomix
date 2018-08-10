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
package io.atomix.protocols.gossip;

import com.google.common.annotations.Beta;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;

import java.util.Collection;

/**
 * Peer selector.
 */
@Beta
@FunctionalInterface
public interface PeerSelector<E> {

  /**
   * Selects the peers to update for the given entry.
   *
   * @param entry the entry for which to select peers
   * @param membership the cluster membership service
   * @return a collection of peers to update
   */
  Collection<MemberId> select(E entry, ClusterMembershipService membership);

}
