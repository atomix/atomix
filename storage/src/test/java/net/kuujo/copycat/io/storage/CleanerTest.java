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
package net.kuujo.copycat.io.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;

import org.testng.annotations.Test;

import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.ServiceLoaderTypeResolver;

/**
 * Minor compaction test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class CleanerTest extends AbstractLogTest {

  protected Log createLog() {
    return tempStorageBuilder()
        .withMaxEntriesPerSegment(10)
        .withSerializer(new Serializer(new ServiceLoaderTypeResolver()))
        .build()
        .open();
  }
  
  /**
   * Tests compacting the log.
   */
  public void testCompact() throws Throwable {
    writeEntries(30);

    assertEquals(log.length(), 30L);

    for (long index = 21; index < 28; index++) {
      log.clean(index);
    }

    CountDownLatch latch = new CountDownLatch(1);
    log.cleaner().clean().thenRun(latch::countDown);
    latch.await();

    assertEquals(log.length(), 30L);

    for (long index = 21; index < 28; index++) {
      assertTrue(log.lastIndex() >= index);
      assertFalse(log.contains(index));
      try (TestEntry entry = log.get(index)) {
        assertNull(entry);
      }
    }
  }

  /**
   * Writes a set of session entries to the log.
   */
  private void writeEntries(int entries) {
    for (int i = 0; i < entries; i++) {
      try (TestEntry entry = log.create(TestEntry.class)) {
        entry.setTerm(1);
        entry.setRemove(false);
        log.append(entry);
      }
    }
  }

}
