// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter;

import io.atomix.core.counter.impl.DefaultAtomicCounterService;
import io.atomix.core.counter.impl.DefaultDistributedCounterBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed counter primitive type.
 */
public class DistributedCounterType implements PrimitiveType<DistributedCounterBuilder, DistributedCounterConfig, DistributedCounter> {
  private static final String NAME = "counter";
  private static final DistributedCounterType INSTANCE = new DistributedCounterType();

  /**
   * Returns a new distributed counter type.
   *
   * @return a new distributed counter type
   */
  public static DistributedCounterType instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultAtomicCounterService();
  }

  @Override
  public DistributedCounterConfig newConfig() {
    return new DistributedCounterConfig();
  }

  @Override
  public DistributedCounterBuilder newBuilder(String name, DistributedCounterConfig config, PrimitiveManagementService managementService) {
    return new DefaultDistributedCounterBuilder(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
