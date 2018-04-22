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
 * Node profiles.
 */
public final class NodeProfiles {

  /**
   * Returns the node profile for the given name
   *
   * @param profileName the name for which to return the node profile
   * @return the node profile for the given name
   */
  public static NamedNodeProfile getNamedProfile(String profileName) {
    for (NamedNodeProfile type : Services.loadAll(NamedNodeProfile.class)) {
      if (type.name().toLowerCase().replace("_", "-").equals(profileName.toLowerCase().replace("_", "-"))) {
        return type;
      }
    }
    throw new ConfigurationException("Unknown node profile: " + profileName);
  }

  private NodeProfiles() {
  }
}
