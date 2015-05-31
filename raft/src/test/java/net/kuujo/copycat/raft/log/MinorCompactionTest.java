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

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.log.entry.KeepAliveEntry;
import net.kuujo.copycat.raft.log.entry.NoOpEntry;
import net.kuujo.copycat.util.ExecutionContext;
import org.testng.annotations.Test;

/**
 * Minor compaction test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class MinorCompactionTest extends ConcurrentTestCase {

  /**
   * Tests compacting the log.
   */
  public void testCompact() throws Throwable {
    Log log = Log.builder()
      .withStorageLevel(StorageLevel.MEMORY)
      .withMaxEntriesPerSegment(128)
      .build();

    ExecutionContext context = new ExecutionContext("test", new Serializer());

    log.open(context);

    writeEntries(log, 550);

    final long index;
    try (NoOpEntry entry = log.createEntry(NoOpEntry.class)) {
      entry.setTerm(1);
      index = log.appendEntry(entry);
    }

    writeEntries(log, 550);

    threadAssertEquals(log.length(), 1101L);

    MinorCompaction compaction = new MinorCompaction(1024, (e, c) -> !(e instanceof NoOpEntry), context);

    expectResume();
    compaction.run(log.segments()).thenRun(this::resume);
    await();

    threadAssertEquals(log.length(), 1101L);
    threadAssertTrue(log.containsIndex(index));
    threadAssertFalse(log.containsEntry(index));

    try (NoOpEntry entry = log.getEntry(index)) {
      threadAssertNull(entry);
    }
  }

  /**
   * Writes a set of session entries to the log.
   */
  private void writeEntries(Log log, int entries) {
    for (int i = 0; i < entries; i++) {
      try (KeepAliveEntry entry = log.createEntry(KeepAliveEntry.class)) {
        entry.setTerm(1);
        entry.setSession(2);
        entry.setTimestamp(System.currentTimeMillis());
        log.appendEntry(entry);
      }
    }
  }

}
