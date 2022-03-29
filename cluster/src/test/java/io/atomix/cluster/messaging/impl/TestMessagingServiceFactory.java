// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import com.google.common.collect.Maps;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;

import java.util.Map;

/**
 * Test messaging service factory.
 */
public class TestMessagingServiceFactory {
  private final Map<Address, TestMessagingService> services = Maps.newConcurrentMap();

  /**
   * Partitions the service at the given address.
   *
   * @param address the address of the service to partition
   */
  public void partition(Address address) {
    TestMessagingService service = services.get(address);
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
    TestMessagingService service = services.get(address);
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
    TestMessagingService service1 = services.get(address1);
    TestMessagingService service2 = services.get(address2);
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
    TestMessagingService service1 = services.get(address1);
    TestMessagingService service2 = services.get(address2);
    service1.heal(service2.address());
    service2.heal(service1.address());
  }

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
