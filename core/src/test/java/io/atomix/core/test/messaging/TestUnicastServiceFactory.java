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
package io.atomix.core.test.messaging;

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
   * Partitions the service at the given address.
   *
   * @param address the address of the service to partition
   */
  public void partition(Address address) {
    TestUnicastService service = services.get(address);
    services.values().stream()
        .filter(s -> !s.address().equals(address))
        .forEach(s -> {
          service.partition(s.address());
          s.partition(service.address());
        });
  }

  /**
   * Heals a partition of the service at the given address.
   *
   * @param address the address of the service to heal
   */
  public void heal(Address address) {
    TestUnicastService service = services.get(address);
    services.values().stream()
        .filter(s -> !s.address().equals(address))
        .forEach(s -> {
          service.heal(s.address());
          s.heal(service.address());
        });
  }

  /**
   * Creates a bi-directional partition between two services.
   *
   * @param address1 the first service
   * @param address2 the second service
   */
  public void partition(Address address1, Address address2) {
    TestUnicastService service1 = services.get(address1);
    TestUnicastService service2 = services.get(address2);
    service1.partition(service2.address());
    service2.partition(service1.address());
  }

  /**
   * Heals a bi-directional partition between two services.
   *
   * @param address1 the first service
   * @param address2 the second service
   */
  public void heal(Address address1, Address address2) {
    TestUnicastService service1 = services.get(address1);
    TestUnicastService service2 = services.get(address2);
    service1.heal(service2.address());
    service2.heal(service1.address());
  }

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
