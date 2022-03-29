// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.buffer;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Heap buffer test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class HeapBufferTest extends BufferTest {

  @Override
  protected Buffer createBuffer(int capacity) {
    return HeapBuffer.allocate(capacity);
  }

  @Override
  protected Buffer createBuffer(int capacity, int maxCapacity) {
    return HeapBuffer.allocate(capacity, maxCapacity);
  }

  @Test
  public void testByteBufferToHeapBuffer() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    byteBuffer.putLong(10);
    byteBuffer.rewind();

    HeapBuffer directBuffer = HeapBuffer.wrap(byteBuffer.array());
    assertEquals(directBuffer.readLong(), byteBuffer.getLong());
  }

  @Test
  public void testDirectToHeapBuffer() {
    DirectBuffer directBuffer = DirectBuffer.allocate(8);
    directBuffer.writeLong(10);
    directBuffer.flip();

    byte[] bytes = new byte[8];
    directBuffer.read(bytes);
    directBuffer.rewind();

    HeapBuffer heapBuffer = HeapBuffer.wrap(bytes);
    assertEquals(directBuffer.readLong(), heapBuffer.readLong());

    directBuffer.release();
  }

  @Test
  public void testHeapToDirectBuffer() {
    HeapBuffer heapBuffer = HeapBuffer.allocate(8);
    heapBuffer.writeLong(10);
    heapBuffer.flip();

    DirectBuffer directBuffer = DirectBuffer.allocate(8);
    directBuffer.write(heapBuffer.array());
    directBuffer.flip();

    assertEquals(directBuffer.readLong(), heapBuffer.readLong());

    directBuffer.release();
  }
}
