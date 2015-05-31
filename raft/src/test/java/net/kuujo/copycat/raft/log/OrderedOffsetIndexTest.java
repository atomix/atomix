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
package net.kuujo.copycat.raft.log;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Ordered offset index test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class OrderedOffsetIndexTest {

  /**
   * Tests indexing an offset and checking whether the index contains the offset.
   */
  public void testIndexContains() {
    OrderedOffsetIndex index = new OrderedOffsetIndex(HeapBuffer.allocate(1024 * 8));
    assertEquals(index.size(), 0);
    assertFalse(index.contains(0));
    index.index(0, 0, 8);
    assertTrue(index.contains(0));
    assertFalse(index.contains(1));
    assertFalse(index.contains(9));
    assertFalse(index.contains(11));
    index.index(1, 1234, 8);
    assertTrue(index.contains(1));
  }

  /**
   * Tests truncating the index.
   */
  public void testIndexTruncate() {
    OffsetIndex index = new OrderedOffsetIndex(HeapBuffer.allocate(1024 * 8));
    assertEquals(index.size(), 0);
    assertFalse(index.contains(0));
    index.index(0, 0, 8);
    index.index(1, 1234, 9);
    index.index(2, 2345, 10);
    index.index(3, 3456, 11);
    assertTrue(index.contains(0));
    assertTrue(index.contains(3));
    assertEquals(index.size(), 4);
    assertEquals(index.length(0), 1234);
    assertEquals(index.length(3), 11);
    index.truncate(2);
    assertEquals(index.size(), 3);
    assertTrue(index.contains(0));
    assertEquals(index.position(0), 0);
    assertEquals(index.length(0), 1234);
    assertTrue(index.contains(2));
    assertEquals(index.position(2), 2345);
    assertEquals(index.length(2), 1111);
    assertFalse(index.contains(3));
  }

  /**
   * Tests recovering the index.
   */
  public void testIndexRecover() {
    Buffer buffer = HeapBuffer.allocate(1024 * 8);
    OrderedOffsetIndex index = new OrderedOffsetIndex(buffer);
    index.index(0, 0, 8);
    index.index(1, 1234, 8);
    index.index(2, 2345, 8);
    assertEquals(index.size(), 3);
    assertEquals(index.lastOffset(), 2);
    OrderedOffsetIndex recover = new OrderedOffsetIndex(buffer);
    assertEquals(recover.size(), 3);
    assertEquals(recover.lastOffset(), 2);
    assertEquals(recover.position(0), 0);
    assertEquals(recover.length(0), 1234);
    assertEquals(recover.position(2), 2345);
    assertEquals(recover.length(2), 8);
  }

}
