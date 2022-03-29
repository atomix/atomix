// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter;

import io.atomix.core.counter.impl.DefaultAtomicCounterBuilder;
import io.atomix.core.counter.impl.DefaultAtomicCounterService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomic counter primitive type.
 */
public class AtomicCounterType implements PrimitiveType<AtomicCounterBuilder, AtomicCounterConfig, AtomicCounter> {
  private static final String NAME = "atomic-counter";
  private static final AtomicCounterType INSTANCE = new AtomicCounterType();

  /**
   * Returns a new atomic counter type.
   *
   * @return a new atomic counter type
   */
  public static AtomicCounterType instance() {
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
  public AtomicCounterConfig newConfig() {
    return new AtomicCounterConfig();
  }

  @Override
  public AtomicCounterBuilder newBuilder(String name, AtomicCounterConfig config, PrimitiveManagementService managementService) {
    return new DefaultAtomicCounterBuilder(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
