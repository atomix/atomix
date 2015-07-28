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
package net.kuujo.copycat.io;

import org.testng.annotations.AfterMethod;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.UUID;

import static org.testng.Assert.*;

/**
 * Mapped buffer test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MappedBufferTest extends BufferTest {
  private File file;

  private File createFile() {
    file = new File(UUID.randomUUID().toString());
    return file;
  }

  @Override
  protected Buffer createBuffer(long capacity) {
    return MappedBuffer.allocate(createFile(), capacity);
  }

  @Override
  protected Buffer createBuffer(long capacity, long maxCapacity) {
    return MappedBuffer.allocate(createFile(), capacity, maxCapacity);
  }

  /**
   * Rests reopening a file that has been closed.
   */
  public void testPersist() {
    File file = createFile();
    try (MappedBuffer buffer = MappedBuffer.allocate(file, 16)) {
      buffer.writeLong(10).writeLong(11).flip();
      assertEquals(buffer.readLong(), 10);
      assertEquals(buffer.readLong(), 11);
    }
    try (MappedBuffer buffer = MappedBuffer.allocate(file, 16)) {
      assertEquals(buffer.readLong(), 10);
      assertEquals(buffer.readLong(), 11);
    }
  }

  /**
   * Tests deleting a file.
   */
  public void testDelete() {
    File file = createFile();
    MappedBuffer buffer = MappedBuffer.allocate(file, 16);
    buffer.writeLong(10).writeLong(11).flip();
    assertEquals(buffer.readLong(), 10);
    assertEquals(buffer.readLong(), 11);
    assertTrue(Files.exists(file.toPath()));
    buffer.delete();
    assertFalse(Files.exists(file.toPath()));
  }

  @AfterMethod
  public void cleanupFile() throws IOException {
    try {
      Files.delete(file.toPath());
    } catch (NoSuchFileException e) {
    }
  }

}
