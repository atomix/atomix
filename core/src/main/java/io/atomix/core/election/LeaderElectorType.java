// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.election;

import io.atomix.core.election.impl.DefaultLeaderElectorService;
import io.atomix.core.election.impl.DefaultLeaderElectorBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Namespace;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Leader elector primitive type.
 */
public class LeaderElectorType<T> implements PrimitiveType<LeaderElectorBuilder<T>, LeaderElectorConfig, LeaderElector<T>> {
  private static final String NAME = "leader-elector";
  private static final LeaderElectorType INSTANCE = new LeaderElectorType();

  /**
   * Returns a new leader elector type.
   *
   * @param <T> the election candidate type
   * @return a new leader elector type
   */
  @SuppressWarnings("unchecked")
  public static <T> LeaderElectorType<T> instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Namespace namespace() {
    return Namespace.builder()
        .register(PrimitiveType.super.namespace())
        .register(Leadership.class)
        .register(Leader.class)
        .build();
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultLeaderElectorService();
  }

  @Override
  public LeaderElectorConfig newConfig() {
    return new LeaderElectorConfig();
  }

  @Override
  public LeaderElectorBuilder<T> newBuilder(String name, LeaderElectorConfig config, PrimitiveManagementService managementService) {
    return new DefaultLeaderElectorBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
