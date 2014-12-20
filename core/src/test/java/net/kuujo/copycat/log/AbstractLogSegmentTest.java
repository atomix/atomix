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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests LogSegment implementations.
 *
 * @author Jonathan Halterman
 */
@Test
public abstract class AbstractLogSegmentTest {
  protected AbstractLog log;
  protected int segmentSize = 100;
  protected int entriesPerSegment = (segmentSize / entrySize()) + 1;

  /**
   * Creates a test log instance.
   */
  protected abstract AbstractLog createLog() throws Throwable;

  /**
   * Deletes the test log instance.
   */
  protected void deleteLog() throws Throwable {
  }

  /** Returns the size of a simple entry */
  protected abstract int entrySize();

  @BeforeMethod
  protected void beforeMethod() throws Throwable {
    log = createLog();
    assertTrue(log.isClosed());
    assertFalse(log.isOpen());
    log.open();
    assertTrue(log.isOpen());
    assertFalse(log.isClosed());
    assertTrue(log.isEmpty());
  }

  @AfterMethod
  protected void afterMethod() throws Throwable {
    try {
      log.close();
      assertFalse(log.isOpen());
      assertTrue(log.isClosed());
    } catch (Exception ignore) {
    } finally {
      log.delete();
    }
  }
 

  public void shouldNotAppendToFullSegment() {
  }
}
