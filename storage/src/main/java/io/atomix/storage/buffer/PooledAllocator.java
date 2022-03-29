// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.buffer;

import io.atomix.utils.concurrent.ReferencePool;

/**
 * Pooled buffer allocator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class PooledAllocator implements BufferAllocator {
  private final ReferencePool<AbstractBuffer> pool;

  protected PooledAllocator(ReferencePool<AbstractBuffer> pool) {
    this.pool = pool;
  }

  /**
   * Returns the maximum buffer capacity.
   *
   * @return The maximum buffer capacity.
   */
  protected abstract int maxCapacity();

  @Override
  public Buffer allocate() {
    return allocate(4096, maxCapacity());
  }

  @Override
  public Buffer allocate(int capacity) {
    return allocate(capacity, maxCapacity());
  }

  @Override
  public Buffer allocate(int initialCapacity, int maxCapacity) {
    return pool.acquire().reset(0, initialCapacity, maxCapacity).clear();
  }

}
