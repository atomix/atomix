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

import io.atomix.utils.AbstractNamed;
import io.atomix.utils.Type;
import io.atomix.utils.config.ConfigurationException;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Profile type configuration.
 */
public class ProfileType extends AbstractNamed implements Type {
  private Class<? extends Profile> profileClass;

  public ProfileType() {
  }

  public ProfileType(String name, Class<? extends Profile> profileClass) {
    super(name);
    this.profileClass = profileClass;
  }

  /**
   * Returns the profile class.
   *
   * @return the profile class
   */
  public Class<? extends Profile> profileClass() {
    return profileClass;
  }

  /**
   * Creates a new profile instance.
   *
   * @return a new profile instance
   */
  public Profile newProfile() {
    try {
      return profileClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to instantiate profile type " + name(), e);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(name());
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof ProfileType && Objects.equals(((ProfileType) object).name(), name());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
