// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.profile;

import java.util.Collection;

/**
 * Profile type registry.
 */
public interface ProfileTypeRegistry {

  /**
   * Returns the collection of all registered profiles.
   *
   * @return the collection of all registered profiles
   */
  Collection<Profile.Type> getProfileTypes();

  /**
   * Returns the profile for the given name.
   *
   * @param name the profile name
   * @return the profile
   */
  Profile.Type getProfileType(String name);

}
