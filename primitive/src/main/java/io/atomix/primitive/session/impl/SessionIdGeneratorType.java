// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.session.impl;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Session ID generator primitive type.
 */
public class SessionIdGeneratorType implements PrimitiveType {
  private static final String NAME = "SESSION_ID_GENERATOR";
  private static final SessionIdGeneratorType TYPE = new SessionIdGeneratorType();

  /**
   * Returns a new session ID generator type.
   *
   * @return a new session ID generator type
   */
  public static SessionIdGeneratorType instance() {
    return TYPE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new SessionIdGeneratorService();
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
