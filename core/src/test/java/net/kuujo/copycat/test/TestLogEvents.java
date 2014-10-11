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
package net.kuujo.copycat.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.kuujo.copycat.log.Entry;

/**
 * Test log events.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestLogEvents {
  private final TestLog log;

  public TestLogEvents(TestLog log) {
    this.log = log;
  }

  /**
   * Listens for an entry being appended to the log.
   *
   * @param entry The entry to match.
   * @return The event object.
   */
  public TestLogEvents appendedEntry(Entry entry) {
    final CountDownLatch latch = new CountDownLatch(1);
    log.addEntryListener((i, e) -> e.equals(entry), latch::countDown);
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  /**
   * Listens for an entry being appended to the log.
   *
   * @param index The index for which to listen.
   * @return The event object.
   */
  public TestLogEvents appendedEntry(long index) {
    final CountDownLatch latch = new CountDownLatch(1);
    log.addEntryListener((i, e) -> i == index, latch::countDown);
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  /**
   * Listens for an entry being appended to the log.
   *
   * @param entryType The entry type.
   * @return The event object.
   */
  public TestLogEvents appendedEntry(Class<? extends Entry> entryType) {
    final CountDownLatch latch = new CountDownLatch(1);
    log.addEntryListener((i, e) -> entryType.isAssignableFrom(e.getClass()), latch::countDown);
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  /**
   * Listens for the log to be compacted.
   *
   * @return The event object.
   */
  public TestLogEvents compacted() {
    final CountDownLatch latch = new CountDownLatch(1);
    log.addCompactListener(latch::countDown);
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

}
