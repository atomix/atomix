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
package io.atomix.core.utils.config;

import io.atomix.core.AtomixRegistry;

/**
 * Polymorphic type mapper.
 */
public abstract class PolymorphicTypeMapper<T> {
  private final Class<? super T> typedClass;

  protected PolymorphicTypeMapper(Class<? super T> typedClass) {
    this.typedClass = typedClass;
  }

  /**
   * Returns the polymorphic type.
   *
   * @return the polymorphic type
   */
  public Class<? super T> getTypedClass() {
    return typedClass;
  }

  /**
   * Returns the type path.
   *
   * @return the type path
   */
  public String getTypePath() {
    return "type";
  }

  /**
   * Returns the concrete configuration class.
   *
   * @param registry the Atomix type registry
   * @param type     the type name
   * @return the concrete configuration class
   */
  public abstract Class<? extends T> getConcreteClass(AtomixRegistry registry, String type);
}
