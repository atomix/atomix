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
package io.atomix.core.profile;

import io.atomix.utils.Services;
import io.atomix.utils.config.ConfigurationException;

/**
 * Atomix profiles.
 */
public final class Profiles {

  /**
   * Returns the Atomix profile for the given name
   *
   * @param profileName the name for which to return the Atomix profile
   * @return the Atomix profile for the given name
   */
  public static NamedProfile getNamedProfile(String profileName) {
    for (NamedProfile type : Services.loadAll(NamedProfile.class)) {
      if (type.name().replace("_", "-").equalsIgnoreCase(profileName.replace("_", "-"))) {
        return type;
      }
    }
    throw new ConfigurationException("Unknown Atomix profile: " + profileName);
  }

  private Profiles() {
  }
}
