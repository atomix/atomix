// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.buffer;

import java.nio.ByteBuffer;

/**
 * {@link ByteBuffer} based direct bytes.
 */
public class DirectBytes extends ByteBufferBytes {

  /**
   * Allocates a new direct byte array.
   *
   * @param size The count of the buffer to allocate (in bytes).
   * @return The direct buffer.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed count for
   *                                  an array on the Java heap - {@code Integer.MAX_VALUE - 5}
   */
  public static DirectBytes allocate(int size) {
    if (size > MAX_SIZE) {
      throw new IllegalArgumentException("size cannot for DirectBytes cannot be greater than " + MAX_SIZE);
    }
    return new DirectBytes(ByteBuffer.allocateDirect((int) size));
  }

  protected DirectBytes(ByteBuffer buffer) {
    super(buffer);
  }

  @Override
  protected ByteBuffer newByteBuffer(int size) {
    return ByteBuffer.allocateDirect((int) size);
  }

  @Override
  public boolean isDirect() {
    return buffer.isDirect();
  }

}
