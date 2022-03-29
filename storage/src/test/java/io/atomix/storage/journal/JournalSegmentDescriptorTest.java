// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Segment descriptor test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class JournalSegmentDescriptorTest {

  /**
   * Tests the segment descriptor builder.
   */
  @Test
  public void testDescriptorBuilder() {
    JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.builder(ByteBuffer.allocate(JournalSegmentDescriptor.BYTES))
        .withId(2)
        .withIndex(1025)
        .withMaxSegmentSize(1024 * 1024)
        .withMaxEntries(2048)
        .build();

    assertEquals(2, descriptor.id());
    assertEquals(JournalSegmentDescriptor.VERSION, descriptor.version());
    assertEquals(1025, descriptor.index());
    assertEquals(1024 * 1024, descriptor.maxSegmentSize());
    assertEquals(2048, descriptor.maxEntries());

    assertEquals(0, descriptor.updated());
    long time = System.currentTimeMillis();
    descriptor.update(time);
    assertEquals(time, descriptor.updated());
  }

  /**
   * Tests copying the segment descriptor.
   */
  @Test
  public void testDescriptorCopy() {
    JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.builder()
        .withId(2)
        .withIndex(1025)
        .withMaxSegmentSize(1024 * 1024)
        .withMaxEntries(2048)
        .build();

    long time = System.currentTimeMillis();
    descriptor.update(time);

    descriptor = descriptor.copyTo(ByteBuffer.allocate(JournalSegmentDescriptor.BYTES));

    assertEquals(2, descriptor.id());
    assertEquals(JournalSegmentDescriptor.VERSION, descriptor.version());
    assertEquals(1025, descriptor.index());
    assertEquals(1024 * 1024, descriptor.maxSegmentSize());
    assertEquals(2048, descriptor.maxEntries());
    assertEquals(time, descriptor.updated());
  }
}
