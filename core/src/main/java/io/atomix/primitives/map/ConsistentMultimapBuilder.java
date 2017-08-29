/*
 * Copyright 2016 Open Networking Foundation
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
 * A builder class for {@code AsyncConsistentMultimap}.
 */
public abstract class ConsistentMultimapBuilder<K, V>
    extends DistributedPrimitiveBuilder<ConsistentMultimapBuilder<K, V>,
    ConsistentMultimap<K, V>> {

  private boolean purgeOnUninstall = false;

  public ConsistentMultimapBuilder() {
    super(DistributedPrimitive.Type.CONSISTENT_MULTIMAP);
  }

  /**
   * Clears multimap contents when the owning application is uninstalled.
   *
   * @return this builder
   */
  public ConsistentMultimapBuilder<K, V> withPurgeOnUninstall() {
    purgeOnUninstall = true;
    return this;
  }

  /**
   * Returns if multimap entries need to be cleared when owning application
   * is uninstalled.
   *
   * @return true if items are to be cleared on uninstall
   */
  public boolean purgeOnUninstall() {
    return purgeOnUninstall;
  }

  /**
   * Builds the distributed multimap based on the configuration options
   * supplied to this builder.
   *
   * @return new distributed multimap
   * @throws RuntimeException if a mandatory parameter is missing
   */
  public abstract AsyncConsistentMultimap<K, V> buildMultimap();
}
