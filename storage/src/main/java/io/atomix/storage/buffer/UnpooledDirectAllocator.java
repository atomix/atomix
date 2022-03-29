// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.buffer;

/**
 * Unpooled direct allocator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnpooledDirectAllocator extends UnpooledAllocator {

  @Override
  public Buffer allocate(int initialCapacity, int maxCapacity) {
    return DirectBuffer.allocate(initialCapacity, maxCapacity);
  }

  @Override
  protected int maxCapacity() {
    return Integer.MAX_VALUE;
  }

}
