// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.log;

import io.atomix.core.log.impl.DefaultDistributedLogBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed log primitive type.
 */
public class DistributedLogType<T> implements PrimitiveType<DistributedLogBuilder<T>, DistributedLogConfig, DistributedLog<T>> {
  private static final String NAME = "log";
  private static final DistributedLogType INSTANCE = new DistributedLogType();

  /**
   * Returns a new distributed log type.
   *
   * @return a new distributed log type
   */
  @SuppressWarnings("unchecked")
  public static <T> DistributedLogType<T> instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DistributedLogConfig newConfig() {
    return new DistributedLogConfig();
  }

  @Override
  public DistributedLogBuilder<T> newBuilder(String name, DistributedLogConfig config, PrimitiveManagementService managementService) {
    return new DefaultDistributedLogBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
