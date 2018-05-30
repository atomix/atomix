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
package io.atomix.core.profile.impl;

import com.google.common.collect.Maps;
import io.atomix.core.profile.ProfileType;
import io.atomix.core.profile.ProfileTypeRegistry;

import java.util.Collection;
import java.util.Map;

/**
 * Default profile type registry.
 */
public class DefaultProfileTypeRegistry implements ProfileTypeRegistry {
  private final Map<String, ProfileType> profileTypes = Maps.newConcurrentMap();

  public DefaultProfileTypeRegistry(Collection<ProfileType> profileTypes) {
    profileTypes.forEach(profileType -> this.profileTypes.put(profileType.name(), profileType));
  }

  @Override
  public Collection<ProfileType> getProfileTypes() {
    return profileTypes.values();
  }

  @Override
  public ProfileType getProfileType(String name) {
    return profileTypes.get(name);
  }

  @Override
  public void addProfileType(ProfileType type) {
    profileTypes.putIfAbsent(type.name(), type);
  }
}
