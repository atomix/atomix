// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock;

import io.atomix.core.lock.impl.DefaultAtomicLockBuilder;
import io.atomix.core.lock.impl.DefaultAtomicLockService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomic lock primitive type.
 */
public class AtomicLockType implements PrimitiveType<AtomicLockBuilder, AtomicLockConfig, AtomicLock> {
  private static final String NAME = "atomic-lock";
  private static final AtomicLockType INSTANCE = new AtomicLockType();

  /**
   * Returns a new distributed lock type.
   *
   * @return a new distributed lock type
   */
  public static AtomicLockType instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultAtomicLockService();
  }

  @Override
  public AtomicLockConfig newConfig() {
    return new AtomicLockConfig();
  }

  @Override
  public AtomicLockBuilder newBuilder(String name, AtomicLockConfig config, PrimitiveManagementService managementService) {
    return new DefaultAtomicLockBuilder(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
