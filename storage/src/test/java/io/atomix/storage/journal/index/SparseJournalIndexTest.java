// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
    index.truncate(4);
    assertNull(index.lookup(4));
    assertNull(index.lookup(8));

    index = new SparseJournalIndex(.2);
    assertNull(index.lookup(100));
    index.index(101, 2);
    assertNull(index.lookup(1));
    index.index(102, 4);
    index.index(103, 6);
    index.index(104, 8);
    index.index(105, 10);
    assertEquals(105, index.lookup(105).index());
    assertEquals(10, index.lookup(105).position());
    index.index(106, 12);
    index.index(107, 14);
    index.index(108, 16);
    assertEquals(105, index.lookup(108).index());
    assertEquals(10, index.lookup(108).position());
    index.index(109, 18);
    index.index(110, 20);
    assertEquals(110, index.lookup(110).index());
    assertEquals(20, index.lookup(110).position());
    index.truncate(108);
    assertEquals(105, index.lookup(108).index());
    assertEquals(10, index.lookup(108).position());
    assertEquals(105, index.lookup(110).index());
    assertEquals(10, index.lookup(110).position());
    index.truncate(104);
    assertNull(index.lookup(104));
    assertNull(index.lookup(108));
  }
}
