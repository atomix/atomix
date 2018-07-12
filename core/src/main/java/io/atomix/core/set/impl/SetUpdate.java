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
package io.atomix.core.set.impl;

import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Set update.
 */
public class SetUpdate<E> {

  public enum Type {
    ADD,
    REMOVE,
    CONTAINS,
    NOT_CONTAINS
  }

  private final Type type;
  private final E element;

  public SetUpdate(Type type, E element) {
    this.type = type;
    this.element = element;
  }

  /**
   * Returns the set update type.
   *
   * @return the set update type
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the set update element.
   *
   * @return the set update element
   */
  public E element() {
    return element;
  }

  /**
   * Maps the value of the update to another type.
   *
   * @param mapper the mapper with which to transform the value
   * @param <T> the type to which to transform the value
   * @return the transformed set update
   */
  public <T> SetUpdate<T> map(Function<E, T> mapper) {
    return new SetUpdate<T>(type, mapper.apply(element));
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type)
        .add("element", element)
        .toString();
  }
}
