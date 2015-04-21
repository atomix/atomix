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

import net.kuujo.copycat.io.HeapBuffer;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.raft.storage.compact.KeyTable;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Key table test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class KeyTableTest {

  /**
   * Tests that the key table size is incremented as entries are added.
   */
  public void testSize() {
    KeyTable table = new KeyTable(1000, HashFunctions.CITYHASH);
    assertEquals(table.size(), 0);
    table.update(HeapBuffer.allocate(8).writeLong(1234).flip(), 1234);
    assertEquals(table.size(), 1);
    table.update(HeapBuffer.allocate(8).writeLong(2345).flip(), 2345);
    assertEquals(table.size(), 2);
    table.update(HeapBuffer.allocate(8).writeLong(1234).flip(), 3456);
    assertEquals(table.size(), 2);
  }

  /**
   * Tests looking up an offset in the key table.
   */
  public void testLookup() {
    KeyTable table = new KeyTable(1000, HashFunctions.CITYHASH);
    assertEquals(table.lookup(HeapBuffer.allocate(8).writeLong(9876).flip()), -1);
    assertEquals(table.update(HeapBuffer.allocate(8).writeLong(1234).flip(), 4321), -1);
    assertEquals(table.lookup(HeapBuffer.allocate(8).writeLong(1234).flip()), 4321);
    assertEquals(table.update(HeapBuffer.allocate(8).writeLong(1234).flip(), 1234), -1);
    assertEquals(table.lookup(HeapBuffer.allocate(8).writeLong(1234).flip()), 4321);
    assertEquals(table.update(HeapBuffer.allocate(8).writeLong(1234).flip(), 5678), 4321);
    assertEquals(table.lookup(HeapBuffer.allocate(8).writeLong(1234).flip()), 5678);
  }

}
