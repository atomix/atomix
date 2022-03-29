// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.semaphore.impl;

import io.atomix.core.semaphore.DistributedSemaphoreType;

/**
 * Default distributed semaphore service.
 */
public class DefaultDistributedSemaphoreService extends AbstractAtomicSemaphoreService {
  public DefaultDistributedSemaphoreService(AtomicSemaphoreServiceConfig config) {
    super(DistributedSemaphoreType.instance(), config.initialCapacity());
  }
}
