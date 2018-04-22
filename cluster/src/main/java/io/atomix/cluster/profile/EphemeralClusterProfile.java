/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.cluster.profile;

import io.atomix.cluster.ClusterConfig;
import io.atomix.cluster.Node;

/**
 * Ephemeral cluster profile.
 */
public class EphemeralClusterProfile implements NamedClusterProfile {
  private static final String NAME = "ephemeral";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void configure(ClusterConfig config) {
    config.getLocalNode().setType(Node.Type.EPHEMERAL);
    config.getNodes().forEach(node -> node.setType(Node.Type.EPHEMERAL));
  }
}
