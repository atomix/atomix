// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
