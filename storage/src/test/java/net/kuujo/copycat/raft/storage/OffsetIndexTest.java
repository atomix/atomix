/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft.storage;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Offset index test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class OffsetIndexTest {

  /**
   * Tests indexing an offset and checking whether the index contains the offset.
   */
  public void testIndexContains() {
    OffsetIndex index = new OffsetIndex(HeapBuffer.allocate(OffsetIndex.bytes(1024)), 1024);
    assertFalse(index.contains(10));
    index.index(10, 1234, 8);
    assertTrue(index.contains(10));
    assertFalse(index.contains(9));
    assertFalse(index.contains(11));
  }

  /**
   * Tests reading the position and length of an offset.
   */
  public void testIndexPositionAndLength() {
    OffsetIndex index = new OffsetIndex(HeapBuffer.allocate(OffsetIndex.bytes(1024)), 1024);
    index.index(1, 0, 8);
    assertEquals(index.position(1), 0);
    assertEquals(index.length(1), 8);
    assertEquals(index.position(10), -1);
    index.index(10, 1234, 8);
    assertEquals(index.position(10), 1234);
    assertEquals(index.length(10), 8);
    index.index(11, 1244, 8);
    assertEquals(index.position(11), 1244);
    assertEquals(index.length(10), 10);
    assertEquals(index.length(11), 8);
    index.index(12, 3456, 8);
    index.index(13, 4567, 8);
    assertEquals(index.position(12), 3456);
    assertEquals(index.position(13), 4567);
  }

  /**
   * Tests deleting an offset.
   */
  public void testIndexDelete() {
    OffsetIndex index = new OffsetIndex(HeapBuffer.allocate(OffsetIndex.bytes(1024)), 1024);
    index.index(10, 1234, 8);
    index.index(11, 2345, 8);
    index.index(12, 3456, 8);
    assertTrue(index.contains(11));
    index.delete(11);
    assertFalse(index.contains(11));
  }

  /**
   * Tests recovering the index.
   */
  public void testIndexRecover() {
    Buffer buffer = HeapBuffer.allocate(OffsetIndex.bytes(1024));
    OffsetIndex index = new OffsetIndex(buffer, 1024);
    index.index(10, 1234, 8);
    index.index(11, 2345, 8);
    index.index(12, 3456, 8);
    assertEquals(index.size(), 3);
    assertEquals(index.lastOffset(), 12);
    buffer.rewind();
    OffsetIndex recover = new OffsetIndex(buffer, 1024);
    assertEquals(recover.size(), 3);
    assertEquals(recover.lastOffset(), 12);
    assertEquals(recover.position(12), 3456);
  }

}
