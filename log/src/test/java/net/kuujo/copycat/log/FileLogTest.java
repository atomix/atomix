/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.log;

import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

/**
 * Buffered log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class FileLogTest extends AbstractLogTest {

  /**
   * Tests configuring the buffered log.
   */
  public void testConfigurationDefaults() throws Throwable {
    Log log = new FileLog();
    assertEquals(log.getSegmentSize(), 1024 * 1024 * 1024);
    log.setSegmentSize(1024 * 1024);
    assertEquals(log.getSegmentSize(), 1024 * 1024);
    assertEquals(log.getSegmentInterval(), Long.MAX_VALUE);
    log.setSegmentInterval(60000);
    assertEquals(log.getSegmentInterval(), 60000);
  }

  /**
   * Tests configuring the buffered log via a configuration file.
   */
  public void testConfigurationFile() throws Throwable {
    Log log = new FileLog("log-test");
    assertEquals(log.getSegmentSize(), 1024 * 1024);
    assertEquals(log.getSegmentInterval(), 60000);
  }

  @AfterTest
  protected void cleanLogDir() throws IOException {
    Path directory = Paths.get("target/test-logs/");
    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  @Override
  protected AbstractLogManager createLog() throws Throwable {
    String id = UUID.randomUUID().toString();
    return (AbstractLogManager) new FileLog()
      .withSegmentSize(segmentSize)
      .withDirectory(new File(String.format("target/test-logs/%s", id)))
      .getLogManager(id);
  }

}
