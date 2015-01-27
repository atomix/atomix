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
import static org.testng.Assert.*;

/**
 * Buffered log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class BufferedLogTest extends AbstractLogTest {

  /**
   * Tests configuring the buffered log.
   */
  public void testConfigurationDefaults() throws Throwable {
    Log log = new BufferedLog();
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
    Log log = new BufferedLog("log-test");
    assertEquals(log.getSegmentSize(), 1024 * 1024);
    assertEquals(log.getSegmentInterval(), 60000);
  }

  @Override
  protected AbstractLogManager createLog() throws Throwable {
    return (AbstractLogManager) new BufferedLog().withSegmentSize(segmentSize).getLogManager("test");
  }

  @Override
  protected int entrySize() {
    return 4;
  }

}
