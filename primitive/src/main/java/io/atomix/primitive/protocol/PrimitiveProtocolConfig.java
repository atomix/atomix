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
package io.atomix.primitive.protocol;

import io.atomix.utils.config.Config;

/**
 * Primitive protocol configuration.
 */
public abstract class PrimitiveProtocolConfig<C extends PrimitiveProtocolConfig<C>> implements Config {
  private String group;

  /**
   * Returns the protocol group.
   *
   * @return the protocol group
   */
  public String getGroup() {
    return group;
  }

  /**
   * Sets the protocol group.
   *
   * @param group the protocol group
   * @return the protocol configuration
   */
  @SuppressWarnings("unchecked")
  public C setGroup(String group) {
    this.group = group;
    return (C) this;
  }
}
