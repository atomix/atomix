/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.election;

import io.atomix.cluster.NodeId;
import io.atomix.core.PrimitiveTypes;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

/**
 * Builder for constructing new {@link AsyncLeaderElection} instances.
 */
public abstract class LeaderElectionBuilder<T>
    extends DistributedPrimitiveBuilder<LeaderElectionBuilder<T>, LeaderElectionConfig, LeaderElection<T>> {

  public LeaderElectionBuilder(String name, LeaderElectionConfig config) {
    super(PrimitiveTypes.leaderElection(), name, config);
  }

  @Override
  public Serializer serializer() {
    Serializer serializer = config.getSerializer();
    if (serializer == null) {
      serializer = Serializer.using(KryoNamespace.builder()
          .register(KryoNamespaces.BASIC)
          .register(NodeId.class)
          .build());
    }
    return serializer;
  }
}
