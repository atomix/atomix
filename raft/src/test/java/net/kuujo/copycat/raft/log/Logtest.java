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

import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.log.entry.CommandEntry;
import net.kuujo.copycat.util.ExecutionContext;
import org.testng.annotations.Test;

import java.io.Serializable;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class LogTest {

  /**
   * Creates a test execution context.
   */
  private ExecutionContext createContext() {
    return new ExecutionContext("test", new Serializer());
  }

  /**
   * Tests writing and reading an entry.
   */
  public void testCreateReadEntry() {
    Log log = Log.builder()
      .withStorageLevel(StorageLevel.MEMORY)
      .withMaxEntrySize(1024)
      .withMaxSegmentSize(1024 * 1024)
      .withMaxEntriesPerSegment(1024)
      .build();

    log.open(createContext());

    assertTrue(log.isOpen());

    long index;
    try (CommandEntry entry = log.createEntry(CommandEntry.class)) {
      entry.setSession(10);
      entry.setRequest(100);
      entry.setResponse(99);
      entry.setCommand(new TestCommand(1));
      index = log.appendEntry(entry);
    }

    try (CommandEntry entry = log.getEntry(index)) {
      assertEquals(entry.getSession(), 10);
      assertEquals(entry.getRequest(), 100);
      assertEquals(entry.getResponse(), 99);
      assertEquals(((TestCommand) entry.getCommand()).id, 1);
    }

    log.close();
  }

  /**
   * Test command.
   */
  public static class TestCommand implements Command<Object>, Serializable {
    private long id;

    public TestCommand() {
    }

    private TestCommand(long id) {
      this.id = id;
    }
  }

}
