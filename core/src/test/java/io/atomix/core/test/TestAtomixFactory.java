// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.test;

import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.atomix.core.test.messaging.TestBroadcastServiceFactory;
import io.atomix.core.test.messaging.TestMessagingServiceFactory;
import io.atomix.core.test.messaging.TestUnicastServiceFactory;
import io.atomix.utils.net.Address;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test Atomix factory.
 */
public class TestAtomixFactory {
  private final TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();
  private final TestUnicastServiceFactory unicastServiceFactory = new TestUnicastServiceFactory();
  private final TestBroadcastServiceFactory broadcastServiceFactory = new TestBroadcastServiceFactory();
  private final AtomicInteger memberId = new AtomicInteger();

  /**
   * Returns a new Atomix instance.
   *
   * @return a new Atomix instance
   */
  public Atomix newInstance() {
    int id = memberId.incrementAndGet();
    return new TestAtomix(
        MemberId.from(String.valueOf(id)),
        Address.from("localhost", 5000 + id),
        messagingServiceFactory,
        unicastServiceFactory,
        broadcastServiceFactory);
  }
}
