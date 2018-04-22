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

import io.atomix.utils.Services;
import io.atomix.utils.config.ConfigurationException;

/**
 * Cluster profiles.
 */
public final class ClusterProfiles {

  /**
   * Returns the cluster profile for the given name
   *
   * @param profileName the name for which to return the cluster profile
   * @return the cluster profile for the given name
   */
  public static NamedClusterProfile getNamedProfile(String profileName) {
    for (NamedClusterProfile type : Services.loadAll(NamedClusterProfile.class)) {
      if (type.name().toLowerCase().replace("_", "-").equals(profileName.toLowerCase().replace("_", "-"))) {
        return type;
      }
    }
    throw new ConfigurationException("Unknown cluster profile: " + profileName);
  }

  private ClusterProfiles() {
  }
}
