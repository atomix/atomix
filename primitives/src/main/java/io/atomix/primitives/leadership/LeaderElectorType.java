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
package io.atomix.primitives.leadership;

import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitives.leadership.impl.DiscreteLeaderElectorBuilder;
import io.atomix.primitives.leadership.impl.LeaderElectorService;

/**
 * Leader elector primitive type.
 */
public class LeaderElectorType<T> implements PrimitiveType<LeaderElectorBuilder<T>, LeaderElector<T>, AsyncLeaderElector<T>> {
  private static final String NAME = "LEADER_ELECTOR";

  /**
   * Returns a new leader elector type.
   *
   * @param <T> the election candidate type
   * @return a new leader elector type
   */
  public static <T> LeaderElectorType<T> instance() {
    return new LeaderElectorType<>();
  }

  private LeaderElectorType() {
  }

  @Override
  public String id() {
    return NAME;
  }

  @Override
  public PrimitiveService newService() {
    return new LeaderElectorService();
  }

  @Override
  public LeaderElectorBuilder<T> newPrimitiveBuilder(String name, PrimitiveClient client) {
    return new DiscreteLeaderElectorBuilder<>(name, client);
  }
}
