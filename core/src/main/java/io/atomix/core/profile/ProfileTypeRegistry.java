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

import java.util.Collection;

/**
 * Profile type registry.
 */
public interface ProfileTypeRegistry {

  /**
   * Returns the collection of all registered profile types.
   *
   * @return the collection of all registered profile types
   */
  Collection<ProfileType> getProfileTypes();

  /**
   * Returns the profile type for the given name.
   *
   * @param name the profile type name
   * @return the profile type
   */
  ProfileType getProfileType(String name);

  /**
   * Adds a new profile type to the registry.
   *
   * @param type the profile type to add
   */
  void addProfileType(ProfileType type);

}
