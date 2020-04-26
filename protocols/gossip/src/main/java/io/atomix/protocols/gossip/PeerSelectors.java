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
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Configurable peer selectors.
 */
@Beta
public enum PeerSelectors implements PeerSelector {
  RANDOM {
    @Override
    public Collection<MemberId> select(Object entry, ClusterMembershipService membership) {
      List<MemberId> sortedMembers = membership.getMembers()
          .stream()
          .map(Member::id)
          .sorted()
          .collect(Collectors.toList());
      return Collections.singletonList(sortedMembers.get((int) Math.floor(Math.random() * sortedMembers.size())));
    }
  }
}
