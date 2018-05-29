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
package io.atomix.utils.config;

import io.atomix.utils.Type;

/**
 * Polymorphic type mapper.
 */
public abstract class PolymorphicTypeMapper<T, U extends Type> {
  private final Class<? super T> typedClass;
  private final Class<? super U> typeClass;

  protected PolymorphicTypeMapper(Class<? super T> typedClass, Class<? super U> typeClass) {
    this.typedClass = typedClass;
    this.typeClass = typeClass;
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
   * Returns the type class.
   *
   * @return the type class
   */
  public Class<? super U> getTypeClass() {
    return typeClass;
  }

  /**
   * Returns the type path.
   *
   * @return the type path
   */
  public String getTypedPath() {
    return "type";
  }

  /**
   * Returns the type path.
   *
   * @param typeName the type name
   * @return the type path
   */
  public abstract String getTypePath(String typeName);

  /**
   * Returns the concrete configuration class.
   *
   * @param type the type instance
   * @return the concrete configuration class
   */
  public abstract Class<? extends T> getConcreteTypedClass(U type);
}
