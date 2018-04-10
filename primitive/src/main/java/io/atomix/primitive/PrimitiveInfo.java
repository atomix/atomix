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
package io.atomix.primitive;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed primitive info.
 */
public final class PrimitiveInfo {
  private final String name;
  private final PrimitiveType type;

  public PrimitiveInfo(String name, PrimitiveType type) {
    this.name = name;
    this.type = type;
  }

  /**
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the primitive type.
   *
   * @return the primitive type
   */
  public PrimitiveType type() {
    return type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PrimitiveInfo) {
      PrimitiveInfo info = (PrimitiveInfo) object;
      return Objects.equals(name, info.name) && Objects.equals(type, info.type);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .toString();
  }
}
