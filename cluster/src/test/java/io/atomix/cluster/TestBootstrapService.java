// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import io.atomix.cluster.messaging.BroadcastService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.cluster.messaging.UnicastService;

/**
 * Test bootstrap service.
 */
public class TestBootstrapService implements BootstrapService {
  private final MessagingService messagingService;
  private final UnicastService unicastService;
  private final BroadcastService broadcastService;

  public TestBootstrapService(MessagingService messagingService, UnicastService unicastService, BroadcastService broadcastService) {
    this.messagingService = messagingService;
    this.unicastService = unicastService;
    this.broadcastService = broadcastService;
  }

  @Override
  public MessagingService getMessagingService() {
    return messagingService;
  }

  @Override
  public UnicastService getUnicastService() {
    return unicastService;
  }

  @Override
  public BroadcastService getBroadcastService() {
    return broadcastService;
  }
}
