// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.profile.impl;

import io.atomix.core.profile.Profile;
import io.atomix.core.profile.ProfileTypeRegistry;

import java.util.Collection;
import java.util.Map;

/**
 * Profile type registry.
 */
public class DefaultProfileTypeRegistry implements ProfileTypeRegistry {
  private final Map<String, Profile.Type> profileTypes;

  public DefaultProfileTypeRegistry(Map<String, Profile.Type> profileTypes) {
    this.profileTypes = profileTypes;
  }

  @Override
  public Collection<Profile.Type> getProfileTypes() {
    return profileTypes.values();
  }

  @Override
  public Profile.Type getProfileType(String name) {
    return profileTypes.get(name);
  }
}
