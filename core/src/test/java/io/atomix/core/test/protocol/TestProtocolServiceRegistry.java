// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.test.protocol;

import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.concurrent.ThreadPoolContext;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Test protocol service registry.
 */
public class TestProtocolServiceRegistry {
  private final ScheduledExecutorService threadPool;
  private final Map<PartitionId, Map<String, TestProtocolService>> partitions = Maps.newConcurrentMap();

  TestProtocolServiceRegistry(ScheduledExecutorService threadPool) {
    this.threadPool = threadPool;
  }

  /**
   * Gets or creates a test service.
   *
   * @param partitionId the partition identifier
   * @param name the service name
   * @param type the service type
   * @param config the service configuration
   * @return the test service
   */
  public TestProtocolService getOrCreateService(PartitionId partitionId, String name, PrimitiveType type, ServiceConfig config) {
    return partitions.computeIfAbsent(partitionId, id -> Maps.newConcurrentMap())
        .computeIfAbsent(name, n ->
            new TestProtocolService(partitionId, n, type, config, type.newService(config), this, new ThreadPoolContext(threadPool)));
  }

  /**
   * Removes the given service.
   *
   * @param partitionId the partition identifier
   * @param name the service name
   */
  public void removeService(PartitionId partitionId, String name) {
    Map<String, TestProtocolService> services = partitions.get(partitionId);
    if (services != null) {
      services.remove(name);
    }
  }
}
