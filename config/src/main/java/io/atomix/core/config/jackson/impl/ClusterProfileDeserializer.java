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
package io.atomix.core.config.jackson.impl;

import io.atomix.cluster.profile.ClusterProfile;
import io.atomix.cluster.profile.ClusterProfiles;

/**
 * Cluster profile deserializer.
 */
public class ClusterProfileDeserializer extends PolymorphicTypeDeserializer<ClusterProfile> {
  @SuppressWarnings("unchecked")
  public ClusterProfileDeserializer() {
    super(ClusterProfile.class, name -> ClusterProfiles.getNamedProfile(name).getClass());
  }
}