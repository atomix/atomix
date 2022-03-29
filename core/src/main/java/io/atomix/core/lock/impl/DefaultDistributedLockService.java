// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock.impl;

import io.atomix.core.lock.DistributedLockType;

/**
 * Default distributed lock service.
 */
public class DefaultDistributedLockService extends AbstractAtomicLockService {
  public DefaultDistributedLockService() {
    super(DistributedLockType.instance());
  }
}
