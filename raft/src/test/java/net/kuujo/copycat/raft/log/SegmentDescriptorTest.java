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

import static org.testng.Assert.*;

/**
 * Segment descriptor test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class SegmentDescriptorTest {

  public void testDescriptorBuilder() {
    SegmentDescriptor descriptor = SegmentDescriptor.builder()
      .withId(2)
      .withVersion(3)
      .withIndex(1025)
      .withRange(1024)
      .withMaxEntrySize(2048)
      .withMaxSegmentSize(1024 * 1024)
      .build();

    assertEquals(descriptor.id(), 2);
    assertEquals(descriptor.version(), 3);
    assertEquals(descriptor.index(), 1025);
    assertEquals(descriptor.range(), 1024);
    assertEquals(descriptor.maxEntrySize(), 2048);
    assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);

    assertEquals(descriptor.updated(), 0);
    long time = System.currentTimeMillis();
    descriptor.update(time);
    assertEquals(descriptor.updated(), time);

    assertFalse(descriptor.locked());
    descriptor.lock();
    assertTrue(descriptor.locked());
  }

  public void testDescriptorPersist() {
    Buffer buffer = HeapBuffer.allocate(SegmentDescriptor.BYTES);
    SegmentDescriptor descriptor = SegmentDescriptor.builder(buffer)
      .withId(2)
      .withVersion(3)
      .withIndex(1025)
      .withRange(1024)
      .withMaxEntrySize(2048)
      .withMaxSegmentSize(1024 * 1024)
      .build();

    assertEquals(descriptor.id(), 2);
    assertEquals(descriptor.version(), 3);
    assertEquals(descriptor.index(), 1025);
    assertEquals(descriptor.range(), 1024);
    assertEquals(descriptor.maxEntrySize(), 2048);
    assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);

    descriptor = new SegmentDescriptor(buffer.rewind());

    assertEquals(descriptor.id(), 2);
    assertEquals(descriptor.version(), 3);
    assertEquals(descriptor.index(), 1025);
    assertEquals(descriptor.range(), 1024);
    assertEquals(descriptor.maxEntrySize(), 2048);
    assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);
  }

  public void testDescriptorCopy() {
    SegmentDescriptor descriptor = SegmentDescriptor.builder()
      .withId(2)
      .withVersion(3)
      .withIndex(1025)
      .withRange(1024)
      .withMaxEntrySize(2048)
      .withMaxSegmentSize(1024 * 1024)
      .build();

    long time = System.currentTimeMillis();
    descriptor.update(time);
    descriptor.lock();

    descriptor = descriptor.copyTo(HeapBuffer.allocate(SegmentDescriptor.BYTES));

    assertEquals(descriptor.id(), 2);
    assertEquals(descriptor.version(), 3);
    assertEquals(descriptor.index(), 1025);
    assertEquals(descriptor.range(), 1024);
    assertEquals(descriptor.maxEntrySize(), 2048);
    assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);
    assertEquals(descriptor.updated(), time);
    assertTrue(descriptor.locked());
  }

}
