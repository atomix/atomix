// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition.impl;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primary elector primitive type.
 */
public class PrimaryElectorType implements PrimitiveType {
  private static final String NAME = "PRIMARY_ELECTOR";
  private static final PrimaryElectorType TYPE = new PrimaryElectorType();

  /**
   * Returns a new primary elector type.
   *
   * @return a new primary elector type
   */
  public static PrimaryElectorType instance() {
    return TYPE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new PrimaryElectorService();
  }

  @Override
  public PrimitiveConfig newConfig() {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrimitiveBuilder newBuilder(String name, PrimitiveConfig config, PrimitiveManagementService managementService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
