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
import io.atomix.core.profile.Profile;
import io.atomix.core.profile.ProfileRegistry;
import io.atomix.utils.Services;

import java.util.Collection;
import java.util.Map;

/**
 * Profile type registry that scans the classpath for available profile types.
 */
public class ClasspathScanningProfileRegistry implements ProfileRegistry {
  private final Map<String, Profile> profileTypes = Maps.newConcurrentMap();

  public ClasspathScanningProfileRegistry(ClassLoader classLoader) {
    init(classLoader);
  }

  /**
   * Initializes the registry by scanning the classpath.
   */
  private void init(ClassLoader classLoader) {
    for (Profile profileType : Services.loadTypes(Profile.class, classLoader)) {
      profileTypes.put(profileType.name(), profileType);
    }
  }

  @Override
  public Collection<Profile> getProfiles() {
    return profileTypes.values();
  }

  @Override
  public Profile getProfile(String name) {
    return profileTypes.get(name);
  }
}
