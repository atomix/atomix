/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core;

import com.google.common.collect.Maps;
import io.atomix.utils.net.Address;
import io.atomix.messaging.ManagedMessagingService;

import java.util.Map;

/**
 * Test messaging service factory.
 */
public class TestMessagingServiceFactory {
  private final Map<Address, TestMessagingService> services = Maps.newConcurrentMap();

  /**
   * Returns a new test messaging service for the given address.
   *
   * @param address the address for which to return a messaging service
   * @return the messaging service for the given address
   */
  public ManagedMessagingService newMessagingService(Address address) {
    return new TestMessagingService(address, services);
  }
}
