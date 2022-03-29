// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock.impl;

import io.atomix.core.lock.AtomicLockType;

/**
 * Default atomic lock service.
 */
public class DefaultAtomicLockService extends AbstractAtomicLockService {
  public DefaultAtomicLockService() {
    super(AtomicLockType.instance());
  }
}
