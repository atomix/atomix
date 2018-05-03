/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.election;

import io.atomix.core.election.impl.LeaderElectorProxyBuilder;
import io.atomix.core.election.impl.LeaderElectorService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Leader elector primitive type.
 */
public class LeaderElectorType<T> implements PrimitiveType<LeaderElectorBuilder<T>, LeaderElectorConfig, LeaderElector<T>, ServiceConfig> {
  private static final String NAME = "leader-elector";

  /**
   * Returns a new leader elector type.
   *
   * @param <T> the election candidate type
   * @return a new leader elector type
   */
  public static <T> LeaderElectorType<T> instance() {
    return new LeaderElectorType<>();
  }

  @Override
  public String id() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new LeaderElectorService(config);
  }

  @Override
  public LeaderElectorBuilder<T> newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
    return newPrimitiveBuilder(name, new LeaderElectorConfig(), managementService);
  }

  @Override
  public LeaderElectorBuilder<T> newPrimitiveBuilder(String name, LeaderElectorConfig config, PrimitiveManagementService managementService) {
    return new LeaderElectorProxyBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .toString();
  }
}
