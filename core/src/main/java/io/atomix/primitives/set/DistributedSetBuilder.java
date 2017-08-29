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
import io.atomix.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for distributed set.
 *
 * @param <E> type set elements.
 */
public abstract class DistributedSetBuilder<E> extends DistributedPrimitiveBuilder<DistributedSetBuilder<E>,
    AsyncDistributedSet<E>> {

  private boolean purgeOnUninstall = false;

  public DistributedSetBuilder() {
    super(DistributedPrimitive.Type.SET);
  }

  /**
   * Enables clearing set contents when the owning application is uninstalled.
   *
   * @return this builder
   */
  public DistributedSetBuilder<E> withPurgeOnUninstall() {
    purgeOnUninstall = true;
    return this;
  }

  /**
   * Returns if set contents need to be cleared when owning application is uninstalled.
   *
   * @return {@code true} if yes; {@code false} otherwise.
   */
  public boolean purgeOnUninstall() {
    return purgeOnUninstall;
  }
}
