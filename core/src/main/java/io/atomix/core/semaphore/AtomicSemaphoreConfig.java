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
package io.atomix.core.semaphore;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.PrimitiveConfig;

/**
 * Semaphore configuration.
 */
public class AtomicSemaphoreConfig extends PrimitiveConfig<AtomicSemaphoreConfig> {
  private int initialCapacity;

  @Override
  public PrimitiveType getType() {
    return AtomicSemaphoreType.instance();
  }

  /**
   * Initialize this semaphore with the given permit count.
   * Only the first initialization will be accepted.
   *
   * @param permits initial permits
   * @return configuration
   */
  public AtomicSemaphoreConfig setInitialCapacity(int permits) {
    initialCapacity = permits;
    return this;
  }

  public int initialCapacity() {
    return initialCapacity;
  }
}
