/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.storage.journal;

import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.FileBuffer;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

/**
 * Segment descriptor test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class JournalSegmentDescriptorTest {
  private static final File file = new File("descriptor.log");

  /**
   * Tests the segment descriptor builder.
   */
  @Test
  public void testDescriptorBuilder() {
    JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.newBuilder(FileBuffer.allocate(file, JournalSegmentDescriptor.BYTES))
      .withId(2)
      .withIndex(1025)
      .withMaxSegmentSize(1024 * 1024)
      .withMaxEntries(2048)
      .build();

    assertEquals(descriptor.id(), 2);
    assertEquals(descriptor.version(), JournalSegmentDescriptor.VERSION);
    assertEquals(descriptor.index(), 1025);
    assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);
    assertEquals(descriptor.maxEntries(), 2048);

    assertEquals(descriptor.updated(), 0);
    long time = System.currentTimeMillis();
    descriptor.update(time);
    assertEquals(descriptor.updated(), time);
  }

  /**
   * Tests persisting the segment descriptor.
   */
  @Test
  public void testDescriptorPersist() {
    Buffer buffer = FileBuffer.allocate(file, JournalSegmentDescriptor.BYTES);
    JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.newBuilder(buffer)
      .withId(2)
      .withIndex(1025)
      .withMaxSegmentSize(1024 * 1024)
      .withMaxEntries(2048)
      .build();

    assertEquals(descriptor.id(), 2);
    assertEquals(descriptor.version(), JournalSegmentDescriptor.VERSION);
    assertEquals(descriptor.index(), 1025);
    assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);
    assertEquals(descriptor.maxEntries(), 2048);

    buffer.close();

    descriptor = new JournalSegmentDescriptor(FileBuffer.allocate(file, JournalSegmentDescriptor.BYTES));

    assertEquals(descriptor.id(), 2);
    assertEquals(descriptor.version(), JournalSegmentDescriptor.VERSION);
    assertEquals(descriptor.index(), 1025);
    assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);

    descriptor.close();

    descriptor = new JournalSegmentDescriptor(FileBuffer.allocate(file, JournalSegmentDescriptor.BYTES));

    assertEquals(descriptor.id(), 2);
    assertEquals(descriptor.version(), JournalSegmentDescriptor.VERSION);
    assertEquals(descriptor.index(), 1025);
    assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);
  }

  /**
   * Tests copying the segment descriptor.
   */
  @Test
  public void testDescriptorCopy() {
    JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.newBuilder()
      .withId(2)
      .withIndex(1025)
      .withMaxSegmentSize(1024 * 1024)
      .withMaxEntries(2048)
      .build();

    long time = System.currentTimeMillis();
    descriptor.update(time);

    descriptor = descriptor.copyTo(FileBuffer.allocate(file, JournalSegmentDescriptor.BYTES));

    assertEquals(descriptor.id(), 2);
    assertEquals(descriptor.version(), JournalSegmentDescriptor.VERSION);
    assertEquals(descriptor.index(), 1025);
    assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);
    assertEquals(descriptor.maxEntries(), 2048);
    assertEquals(descriptor.updated(), time);
  }

  /**
   * Deletes the descriptor file.
   */
  @After
  public void deleteDescriptor() throws IOException {
    if (Files.exists(file.toPath())) {
      Files.delete(file.toPath());
    }
  }
}
