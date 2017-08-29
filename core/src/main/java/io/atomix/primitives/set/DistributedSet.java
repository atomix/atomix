/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitives.set;

import io.atomix.primitives.DistributedPrimitive;

import java.util.Set;

/**
 * A distributed collection designed for holding unique elements.
 *
 * @param <E> set entry type
 */
public interface DistributedSet<E> extends Set<E>, DistributedPrimitive {

  /**
   * Registers the specified listener to be notified whenever
   * the set is updated.
   *
   * @param listener listener to notify about set update events
   */
  void addListener(SetEventListener<E> listener);

  /**
   * Unregisters the specified listener.
   *
   * @param listener listener to unregister.
   */
  void removeListener(SetEventListener<E> listener);
}
