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
package net.kuujo.copycat.io.storage;

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
    OffsetIndex index = new OffsetIndex(HeapBuffer.allocate(1024 * 8));
    assertFalse(index.contains(10));
    index.index(10, 1234);
    assertTrue(index.contains(10));
    assertFalse(index.contains(9));
    assertFalse(index.contains(11));
  }

  /**
   * Tests reading the position and length of an offset.
   */
  public void testIndexPositionAndLength() {
    OffsetIndex index = new OffsetIndex(HeapBuffer.allocate(1024 * 8));
    index.index(1, 0);
    assertEquals(index.position(1), 0);
    assertEquals(index.position(10), -1);
    index.index(10, 1234);
    assertEquals(index.position(10), 1234);
    index.index(11, 1244);
    assertEquals(index.position(11), 1244);
    index.index(12, 3456);
    index.index(13, 4567);
    assertEquals(index.position(12), 3456);
    assertEquals(index.position(13), 4567);
  }

  /**
   * Tests truncating entries.
   */
  public void testTruncate() {
    OffsetIndex index = new OffsetIndex(HeapBuffer.allocate(1024 * 8));
    index.index(0, 0);
    index.index(1, 10);
    index.index(2, 20);
    index.index(3, 30);
    index.index(4, 40);
    assertEquals(index.truncate(2), 30);
  }

  /**
   * Tests truncating skipped entries.
   */
  public void testTruncateSkipped() {
    OffsetIndex index = new OffsetIndex(HeapBuffer.allocate(1024 * 8));
    index.index(0, 0);
    index.index(1, 10);
    index.index(3, 30);
    index.index(4, 40);
    assertEquals(index.truncate(2), 30);
  }

  /**
   * Tests truncating a deleted item from the index.
   */
  public void testTruncateDeleted() {
    OffsetIndex index = new OffsetIndex(HeapBuffer.allocate(1024 * 8));
    index.index(0, 0);
    index.index(1, 10);
    index.index(3, 30);
    index.index(4, 40);
    index.delete(1);
    assertEquals(index.truncate(1), 30);
  }

}
