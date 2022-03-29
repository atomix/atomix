// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.semaphore.impl;

import io.atomix.primitive.service.ServiceConfig;

/**
 * Semaphore service configuration.
 */
public class AtomicSemaphoreServiceConfig extends ServiceConfig {
  private int initialCapacity;

  /**
   * Initialize this semaphore with the given permit count.
   * Only the first initialization will be accepted.
   *
   * @param permits initial permits
   * @return configuration
   */
  public AtomicSemaphoreServiceConfig setInitialCapacity(int permits) {
    initialCapacity = permits;
    return this;
  }

  public int initialCapacity() {
    return initialCapacity;
  }
}
