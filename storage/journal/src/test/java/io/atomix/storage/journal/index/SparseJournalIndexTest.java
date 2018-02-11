/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.storage.journal.index;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Sparse journal index test.
 */
public class SparseJournalIndexTest {
  @Test
  public void testSparseJournalIndex() throws Exception {
    JournalIndex index = new SparseJournalIndex(.2);
    assertNull(index.lookup(1));
    index.index(1, 2);
    assertNull(index.lookup(1));
    index.index(2, 4);
    index.index(3, 6);
    index.index(4, 8);
    index.index(5, 10);
    assertEquals(5, index.lookup(5).index());
    assertEquals(10, index.lookup(5).position());
    index.index(6, 12);
    index.index(7, 14);
    index.index(8, 16);
    assertEquals(5, index.lookup(8).index());
    assertEquals(10, index.lookup(8).position());
    index.index(9, 18);
    index.index(10, 20);
    assertEquals(10, index.lookup(10).index());
    assertEquals(20, index.lookup(10).position());
    index.truncate(8);
    assertEquals(5, index.lookup(8).index());
    assertEquals(10, index.lookup(8).position());
    assertEquals(5, index.lookup(10).index());
    assertEquals(10, index.lookup(10).position());
  }
}
