// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.semaphore.impl;

import io.atomix.core.semaphore.AtomicSemaphoreType;

/**
 * Default atomic semaphore service.
 */
public class DefaultAtomicSemaphoreService extends AbstractAtomicSemaphoreService {
  public DefaultAtomicSemaphoreService(AtomicSemaphoreServiceConfig config) {
    super(AtomicSemaphoreType.instance(), config.initialCapacity());
  }
}
