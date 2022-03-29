// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import com.google.common.collect.Sets;
import io.atomix.cluster.messaging.ManagedBroadcastService;

import java.util.Set;

/**
 * Test broadcast service factory.
 */
public class TestBroadcastServiceFactory {
  private final Set<TestBroadcastService> services = Sets.newCopyOnWriteArraySet();

  /**
   * Returns a new test broadcast service for the given endpoint.
   *
   * @return the broadcast service for the given endpoint
   */
  public ManagedBroadcastService newBroadcastService() {
    return new TestBroadcastService(services);
  }
}
