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
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerConfig;

/**
 * Builder for constructing new {@link AsyncLeaderElector} instances.
 */
public abstract class LeaderElectorBuilder<T>
    extends DistributedPrimitiveBuilder<LeaderElectorBuilder<T>, LeaderElectorConfig, LeaderElector<T>> {
  public LeaderElectorBuilder(String name, LeaderElectorConfig config, PrimitiveManagementService managementService) {
    super(LeaderElectorType.instance(), name, config, managementService);
  }

  @Override
  public Serializer serializer() {
    Serializer serializer = this.serializer;
    if (serializer == null) {
      SerializerConfig config = this.config.getSerializerConfig();
      if (config == null) {
        serializer = Serializer.using(KryoNamespace.builder()
            .register(KryoNamespaces.BASIC)
            .register(NodeId.class)
            .build());
      } else {
        serializer = Serializer.using(KryoNamespace.builder()
            .register(KryoNamespaces.BASIC)
            .register(NodeId.class)
            .register(new KryoNamespace(config))
            .build());
      }
    }
    return serializer;
  }
}
