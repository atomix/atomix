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
package io.atomix.primitives.map;

import io.atomix.primitives.DistributedPrimitive;
import io.atomix.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for {@link ConsistentMap} instances.
 *
 * @param <K> type for map key
 * @param <V> type for map value
 */
public abstract class ConsistentMapBuilder<K, V>
    extends DistributedPrimitiveBuilder<ConsistentMapBuilder<K, V>, ConsistentMap<K, V>> {

  private boolean purgeOnUninstall = false;

  public ConsistentMapBuilder() {
    super(DistributedPrimitive.Type.CONSISTENT_MAP);
  }

  /**
   * Clears map contents when the owning application is uninstalled.
   *
   * @return this builder
   */
  public ConsistentMapBuilder<K, V> withPurgeOnUninstall() {
    purgeOnUninstall = true;
    return this;
  }

  /**
   * Returns if map entries need to be cleared when owning application is uninstalled.
   *
   * @return {@code true} if yes; {@code false} otherwise.
   */
  public boolean purgeOnUninstall() {
    return purgeOnUninstall;
  }

  /**
   * Builds an async consistent map based on the configuration options
   * supplied to this builder.
   *
   * @return new async consistent map
   * @throws RuntimeException if a mandatory parameter is missing
   */
  public abstract AsyncConsistentMap<K, V> buildAsyncMap();
}
