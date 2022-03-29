// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.buffer;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Direct buffer test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectBufferTest extends BufferTest {

  @Override
  protected Buffer createBuffer(int capacity) {
    return DirectBuffer.allocate(capacity);
  }

  @Override
  protected Buffer createBuffer(int capacity, int maxCapacity) {
    return DirectBuffer.allocate(capacity, maxCapacity);
  }

  @Test
  public void testByteBufferToDirectBuffer() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    byteBuffer.putLong(10);
    byteBuffer.flip();

    DirectBuffer directBuffer = DirectBuffer.allocate(8);
    directBuffer.write(byteBuffer.array());
    directBuffer.flip();
    assertEquals(directBuffer.readLong(), byteBuffer.getLong());
    assertTrue(directBuffer.isDirect());
    directBuffer.release();
  }

}
