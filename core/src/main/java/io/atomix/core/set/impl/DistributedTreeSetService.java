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

import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

/**
 * Distributed tree set service.
 */
public interface DistributedTreeSetService<E extends Comparable<E>> extends DistributedNavigableSetService<E> {

  /**
   * Returns the size of the given subset.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the descending iterator ID
   */
  @Query("subSetSize")
  int size(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Clears the given view from the set.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   */
  @Command("subSetClear")
  void clear(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Returns a descending iterator.
   *
   * @return the descending iterator ID
   */
  @Command
  long iterateDescending();

  /**
   * Returns a descending iterator.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the descending iterator ID
   */
  @Command("subSetIterate")
  long iterate(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

  /**
   * Returns a descending iterator.
   *
   * @param fromElement low endpoint of the returned set
   * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
   * @param toElement high endpoint of the returned set
   * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
   * @return the descending iterator ID
   */
  @Command("subSetIterateDescending")
  long iterateDescending(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

}
