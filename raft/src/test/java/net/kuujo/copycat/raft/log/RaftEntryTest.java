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

import org.testng.annotations.Test;
import static org.testng.Assert.*;

import java.nio.ByteBuffer;

/**
 * Raft entry test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class RaftEntryTest {

  /**
   * Tests constructing an entry via the entry constructor.
   */
  public void testConstructEntry() {
    RaftEntry entry = new RaftEntry(RaftEntry.Type.COMMAND, 1, ByteBuffer.wrap("Hello world!".getBytes()));
    assertEquals(entry.type(), RaftEntry.Type.COMMAND);
    assertEquals(entry.term(), 1);
    ByteBuffer buffer = entry.entry();
    byte[] bytes = new byte[buffer.limit()];
    buffer.get(bytes);
    assertEquals(bytes, "Hello world!".getBytes());
  }

}
