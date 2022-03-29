// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.barrier.impl;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Cyclic barrier event.
 */
public class CyclicBarrierEvent {
  private final int id;
  private final long version;

  public CyclicBarrierEvent() {
    this(0, 0);
  }

  public CyclicBarrierEvent(int id, long version) {
    this.id = id;
    this.version = version;
  }

  /**
   * Returns the lock ID.
   *
   * @return The lock ID.
   */
  public int id() {
    return id;
  }

  /**
   * Returns the lock version.
   *
   * @return The lock version.
   */
  public long version() {
    return version;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id)
        .add("version", version)
        .toString();
  }
}
