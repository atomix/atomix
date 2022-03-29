// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Pool of reference counted objects.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReferencePool<T extends ReferenceCounted<?>> implements ReferenceManager<T>, AutoCloseable {
  private final ReferenceFactory<T> factory;
  private final Queue<T> pool = new ConcurrentLinkedQueue<>();
  private volatile boolean closed;

  public ReferencePool(ReferenceFactory<T> factory) {
    if (factory == null) {
      throw new NullPointerException("factory cannot be null");
    }
    this.factory = factory;
  }

  /**
   * Acquires a reference.
   *
   * @return The acquired reference.
   */
  public T acquire() {
    if (closed) {
      throw new IllegalStateException("pool closed");
    }

    T reference = pool.poll();
    if (reference == null) {
      reference = factory.createReference(this);
    }
    reference.acquire();
    return reference;
  }

  @Override
  public void release(T reference) {
    if (!closed) {
      pool.add(reference);
    }
  }

  @Override
  public synchronized void close() {
    if (closed) {
      throw new IllegalStateException("pool closed");
    }

    closed = true;
    for (T reference : pool) {
      reference.close();
    }
  }

}
