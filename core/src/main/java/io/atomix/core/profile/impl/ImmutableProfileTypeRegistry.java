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

import com.google.common.collect.ImmutableList;
import io.atomix.core.profile.ProfileType;
import io.atomix.core.profile.ProfileTypeRegistry;

import java.util.Collection;

/**
 * Immutable profile type registry.
 */
public class ImmutableProfileTypeRegistry implements ProfileTypeRegistry {
  private final ProfileTypeRegistry profileTypes;

  public ImmutableProfileTypeRegistry(ProfileTypeRegistry profileTypes) {
    this.profileTypes = profileTypes;
  }

  @Override
  public Collection<ProfileType> getProfileTypes() {
    return ImmutableList.copyOf(profileTypes.getProfileTypes());
  }

  @Override
  public ProfileType getProfileType(String name) {
    return profileTypes.getProfileType(name);
  }

  @Override
  public void addProfileType(ProfileType type) {
    throw new UnsupportedOperationException();
  }
}
