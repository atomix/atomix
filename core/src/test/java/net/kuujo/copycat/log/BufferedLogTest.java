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

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * Buffered log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class BufferedLogTest extends AbstractLogTest {
  @Override
  protected Log createLog() throws Throwable {
    LogConfig config = new LogConfig().withSegmentSize(100);
    return new BufferedLog("test", config);
  }

  /**
   * Tests the buffered log with a zero retention policy.
   */
  public void testWithZeroRetention() {
    LogConfig config = new LogConfig().withSegmentSize(1000).withRetentionPolicy(
      new ZeroRetentionPolicy());

    Log log = new BufferedLog("test", config);
    log.open();
    assertEquals(log.segments().size(), 1);
    for (int i = 0; i < 10; i++) {
      appendEntries(1000);
    }
    assertEquals(log.segments().size(), 1);
  }

  /**
   * Tests the buffered log with a size based retention policy.
   */
  public void testWithSizeRetention() {
    LogConfig config = new LogConfig().withSegmentSize(1000).withRetentionPolicy(
      new SizeBasedRetentionPolicy().withSize(2000));

    Log log = new BufferedLog("test", config);
    log.open();
    assertEquals(log.segments().size(), 1);
    appendEntries(1999);
    assertEquals(log.segments().size(), 2);
    appendEntries(1);
    assertEquals(log.segments().size(), 2);
  }
}
