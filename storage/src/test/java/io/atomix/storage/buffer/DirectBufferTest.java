/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.storage.buffer;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

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

    HeapBuffer directBuffer = HeapBuffer.allocate(8);
    directBuffer.write(byteBuffer.array());
    directBuffer.flip();
    assertEquals(directBuffer.readLong(), byteBuffer.getLong());

    byteBuffer.rewind();
    UnsafeHeapBuffer heapBuffer = UnsafeHeapBuffer.wrap(byteBuffer.array());
    assertEquals(heapBuffer.readLong(), byteBuffer.getLong());
  }

  @Test
  public void testUnsafeDirectToDirectBuffer() {
    UnsafeDirectBuffer unsafeDirectBuffer = UnsafeDirectBuffer.allocate(8);
    unsafeDirectBuffer.writeLong(10);
    unsafeDirectBuffer.flip();

    byte[] bytes = new byte[8];
    unsafeDirectBuffer.read(bytes);
    unsafeDirectBuffer.rewind();

    DirectBuffer directBuffer = DirectBuffer.allocate(8);
    directBuffer.write(bytes);
    directBuffer.flip();
    assertEquals(unsafeDirectBuffer.readLong(), directBuffer.readLong());
  }

  @Test
  public void testDirectToUnsafeDirectBuffer() {
    DirectBuffer directBuffer = DirectBuffer.allocate(8);
    directBuffer.writeLong(10);
    directBuffer.flip();

    byte[] bytes = new byte[8];
    directBuffer.read(bytes);
    directBuffer.rewind();

    UnsafeDirectBuffer unsafeDirectBuffer = UnsafeDirectBuffer.allocate(8);
    unsafeDirectBuffer.write(bytes);
    unsafeDirectBuffer.flip();

    assertEquals(unsafeDirectBuffer.readLong(), directBuffer.readLong());
  }

}
