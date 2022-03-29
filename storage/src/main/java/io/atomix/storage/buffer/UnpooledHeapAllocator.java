// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.buffer;

/**
 * Unpooled heap allocator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnpooledHeapAllocator extends UnpooledAllocator {

  @Override
  protected int maxCapacity() {
    return HeapBuffer.MAX_SIZE;
  }

  @Override
  public Buffer allocate(int initialCapacity, int maxCapacity) {
    return HeapBuffer.allocate(initialCapacity, maxCapacity);
  }

}
