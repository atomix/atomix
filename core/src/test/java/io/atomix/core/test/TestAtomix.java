// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.test;

import io.atomix.cluster.ClusterConfig;
import io.atomix.cluster.MemberConfig;
import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixConfig;
import io.atomix.core.AtomixRegistry;
import io.atomix.core.profile.DataGridProfileConfig;
import io.atomix.core.test.messaging.TestBroadcastServiceFactory;
import io.atomix.core.test.messaging.TestMessagingServiceFactory;
import io.atomix.core.test.messaging.TestUnicastServiceFactory;
import io.atomix.utils.net.Address;

import java.util.Collections;

/**
 * Test Atomix instance.
 */
public class TestAtomix extends Atomix {

  private static AtomixConfig config(MemberId memberId, Address address) {
    return new AtomixConfig()
        .setClusterConfig(new ClusterConfig()
            .setNodeConfig(new MemberConfig()
                .setId(memberId)
                .setAddress(address)))
        .setProfiles(Collections.singletonList(new DataGridProfileConfig()));
  }

  TestAtomix(
      MemberId memberId,
      Address address,
      TestMessagingServiceFactory messagingServiceFactory,
      TestUnicastServiceFactory unicastServiceFactory,
      TestBroadcastServiceFactory broadcastServiceFactory) {
    super(
        config(memberId, address),
        AtomixRegistry.registry(),
        messagingServiceFactory.newMessagingService(address),
        unicastServiceFactory.newUnicastService(address),
        broadcastServiceFactory.newBroadcastService());
  }
}
