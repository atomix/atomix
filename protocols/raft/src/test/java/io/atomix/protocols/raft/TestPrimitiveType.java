// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

/**
 * Test primitive type.
 */
public class TestPrimitiveType implements PrimitiveType {
  private static final TestPrimitiveType INSTANCE = new TestPrimitiveType();

  /**
   * Returns a singleton instance.
   *
   * @return a singleton primitive type instance
   */
  public static TestPrimitiveType instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return "test";
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrimitiveConfig newConfig() {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrimitiveBuilder newBuilder(String primitiveName, PrimitiveConfig config, PrimitiveManagementService managementService) {
    throw new UnsupportedOperationException();
  }
}
