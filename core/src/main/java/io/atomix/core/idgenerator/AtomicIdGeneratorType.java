// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.idgenerator;

import io.atomix.core.counter.impl.DefaultAtomicCounterService;
import io.atomix.core.idgenerator.impl.DelegatingAtomicIdGeneratorBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomic ID generator primitive type.
 */
public class AtomicIdGeneratorType implements PrimitiveType<AtomicIdGeneratorBuilder, AtomicIdGeneratorConfig, AtomicIdGenerator> {
  private static final String NAME = "atomic-id-generator";
  private static final AtomicIdGeneratorType INSTANCE = new AtomicIdGeneratorType();

  /**
   * Returns a new atomic ID generator type.
   *
   * @return a new atomic ID generator type
   */
  public static AtomicIdGeneratorType instance() {
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
  public AtomicIdGeneratorConfig newConfig() {
    return new AtomicIdGeneratorConfig();
  }

  @Override
  public AtomicIdGeneratorBuilder newBuilder(String name, AtomicIdGeneratorConfig config, PrimitiveManagementService managementService) {
    return new DelegatingAtomicIdGeneratorBuilder(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
