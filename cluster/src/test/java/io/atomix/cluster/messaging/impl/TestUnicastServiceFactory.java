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
package io.atomix.cluster.messaging.impl;

import com.google.common.collect.Maps;
import io.atomix.cluster.messaging.ManagedUnicastService;
import io.atomix.utils.net.Address;

import java.util.Map;

/**
 * Test unicast service factory.
 */
public class TestUnicastServiceFactory {
  private final Map<Address, TestUnicastService> services = Maps.newConcurrentMap();

  /**
   * Returns a new test unicast service for the given endpoint.
   *
   * @param address the address to which to bind
   * @return the unicast service for the given endpoint
   */
  public ManagedUnicastService newUnicastService(Address address) {
    return new TestUnicastService(address, services);
  }
}
