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

import io.atomix.core.registry.AtomixRegistry;

/**
 * Abstract profile.
 */
public abstract class AbstractProfile implements Profile {

  /**
   * Finds the profile type for the given profile class.
   *
   * @param profileClass the profile class
   * @return the profile type
   */
  protected static ProfileType findProfileType(Class<? extends Profile> profileClass) {
    return AtomixRegistry.registry(profileClass.getClassLoader())
        .profileTypes()
        .getProfileTypes()
        .stream()
        .filter(type -> type.profileClass() == profileClass)
        .findFirst()
        .orElse(null);
  }
}
