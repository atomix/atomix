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

import io.atomix.cluster.messaging.BroadcastService;
import io.atomix.cluster.messaging.MessagingService;

/**
 * Test bootstrap service.
 */
public class TestBootstrapService implements BootstrapService {
  private final MessagingService messagingService;
  private final BroadcastService broadcastService;

  public TestBootstrapService(MessagingService messagingService, BroadcastService broadcastService) {
    this.messagingService = messagingService;
    this.broadcastService = broadcastService;
  }

  @Override
  public MessagingService getMessagingService() {
    return messagingService;
  }

  @Override
  public BroadcastService getBroadcastService() {
    return broadcastService;
  }
}
