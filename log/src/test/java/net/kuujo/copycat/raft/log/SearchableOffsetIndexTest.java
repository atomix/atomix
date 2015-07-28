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
import net.kuujo.copycat.raft.log.SearchableOffsetIndex;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Offset index test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class SearchableOffsetIndexTest {

  /**
   * Tests indexing an offset and checking whether the index contains the offset.
   */
  public void testIndexContains() {
    SearchableOffsetIndex index = new SearchableOffsetIndex(HeapBuffer.allocate(1024 * 8));
    Assert.assertFalse(index.contains(10));
    index.index(10, 1234, 8);
    Assert.assertTrue(index.contains(10));
    Assert.assertFalse(index.contains(9));
    Assert.assertFalse(index.contains(11));
  }

  /**
   * Tests reading the position and length of an offset.
   */
  public void testIndexPositionAndLength() {
    SearchableOffsetIndex index = new SearchableOffsetIndex(HeapBuffer.allocate(1024 * 8));
    index.index(1, 0, 8);
    Assert.assertEquals(index.position(1), 0);
    Assert.assertEquals(index.length(1), 8);
    Assert.assertEquals(index.position(10), -1);
    index.index(10, 1234, 8);
    Assert.assertEquals(index.position(10), 1234);
    Assert.assertEquals(index.length(10), 8);
    index.index(11, 1244, 8);
    Assert.assertEquals(index.position(11), 1244);
    Assert.assertEquals(index.length(10), 10);
    Assert.assertEquals(index.length(11), 8);
    index.index(12, 3456, 8);
    index.index(13, 4567, 8);
    Assert.assertEquals(index.position(12), 3456);
    Assert.assertEquals(index.position(13), 4567);
  }

  /**
   * Tests recovering the index.
   */
  public void testIndexRecover() {
    Buffer buffer = HeapBuffer.allocate(1024 * 8);
    SearchableOffsetIndex index = new SearchableOffsetIndex(buffer);
    index.index(10, 1234, 8);
    index.index(11, 2345, 8);
    index.index(12, 3456, 8);
    Assert.assertEquals(index.size(), 3);
    Assert.assertEquals(index.lastOffset(), 12);
    buffer.rewind();
    SearchableOffsetIndex recover = new SearchableOffsetIndex(buffer);
    Assert.assertEquals(recover.size(), 3);
    Assert.assertEquals(recover.lastOffset(), 12);
    Assert.assertEquals(recover.position(12), 3456);
  }

}
