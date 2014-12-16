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

import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.testng.Assert.*;

/**
 * Buffered log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class BufferedLogTest {

  /**
   * Tests calculation of the log size.
   */
  public void testSize() {
    LogConfig config = new LogConfig()
      .withSegmentSize(100);

    Log log = new BufferedLog("test", config);
    log.open();
    assertTrue(log.isOpen());
    assertEquals(log.segments().size(), 1);

    appendEntries(log, 100);
    assertEquals(log.segments().size(), 1);
    assertEquals(log.size(), 100);
    assertFalse(log.isEmpty());

    appendEntries(log, 100);
    assertEquals(log.segments().size(), 2);
    assertEquals(log.size(), 200);
    assertFalse(log.isEmpty());

    assertEquals(log.segments().iterator().next().segment(), 1);
    assertEquals(log.segment().segment(), 101);
  }

  /**
   * Tests that the log rotates segments once the segment size has been reached.
   */
  public void testRotateSegments() {
    LogConfig config = new LogConfig()
      .withSegmentSize(1000);

    Log log = new BufferedLog("test", config);
    log.open();
    assertEquals(log.segments().size(), 1);
    appendEntries(log, 10000);
    assertEquals(log.firstIndex(), 1);
    assertEquals(log.lastIndex(), 10000);
    assertEquals(log.segments().size(), 10);
  }

  /**
   * Tests the buffered log with a zero retention policy.
   */
  public void testWithZeroRetention() {
    LogConfig config = new LogConfig()
      .withSegmentSize(1000)
      .withRetentionPolicy(new ZeroRetentionPolicy());

    Log log = new BufferedLog("test", config);
    log.open();
    assertEquals(log.segments().size(), 1);
    for (int i = 0; i < 10; i++) {
      appendEntries(log, 1000);
    }
    assertEquals(log.segments().size(), 1);
  }

  /**
   * Tests the buffered log with a size based retention policy.
   */
  public void testWithSizeRetention() {
    LogConfig config = new LogConfig()
      .withSegmentSize(1000)
      .withRetentionPolicy(new SizeBasedRetentionPolicy().withSize(2000));

    Log log = new BufferedLog("test", config);
    log.open();
    assertEquals(log.segments().size(), 1);
    appendEntries(log, 1999);
    assertEquals(log.segments().size(), 2);
    appendEntries(log, 1);
    assertEquals(log.segments().size(), 2);
  }

  /**
   * Appends a number of 1 byte entries to the log.l
   */
  private void appendEntries(Log log, int numEntries) {
    for (int i = 0; i < numEntries; i++) {
      log.appendEntry(ByteBuffer.wrap(new byte[]{1}));
    }
  }

}
