/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.raft.storage;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Raft storage test.
 */
public class RaftStorageTest {

  @Test
  public void testDefaultConfiguration() throws Exception {
    RaftStorage storage = RaftStorage.builder().build();
    assertEquals("atomix", storage.prefix());
    assertEquals(new File(System.getProperty("user.dir")), storage.directory());
    assertEquals(1024 * 1024 * 32, storage.maxLogSegmentSize());
    assertEquals(1024 * 1024, storage.maxLogEntriesPerSegment());
    assertTrue(storage.dynamicCompaction());
    assertEquals(.2, storage.freeDiskBuffer(), .01);
    assertTrue(storage.isFlushOnCommit());
    assertFalse(storage.isRetainStaleSnapshots());
    assertTrue(storage.statistics().getFreeMemory() > 0);
  }

  @Test
  public void testCustomConfiguration() throws Exception {
    RaftStorage storage = RaftStorage.builder()
        .withPrefix("foo")
        .withDirectory(new File(System.getProperty("user.dir"), "foo"))
        .withMaxSegmentSize(1024 * 1024)
        .withMaxEntriesPerSegment(1024)
        .withDynamicCompaction(false)
        .withFreeDiskBuffer(.5)
        .withFlushOnCommit(false)
        .withRetainStaleSnapshots()
        .build();
    assertEquals("foo", storage.prefix());
    assertEquals(new File(System.getProperty("user.dir"), "foo"), storage.directory());
    assertEquals(1024 * 1024, storage.maxLogSegmentSize());
    assertEquals(1024, storage.maxLogEntriesPerSegment());
    assertFalse(storage.dynamicCompaction());
    assertEquals(.5, storage.freeDiskBuffer(), .01);
    assertFalse(storage.isFlushOnCommit());
    assertTrue(storage.isRetainStaleSnapshots());
  }

  @Test
  public void testCustomConfiguration2() throws Exception {
    RaftStorage storage = RaftStorage.builder()
            .withDirectory(System.getProperty("user.dir") + "/baz")
            .withDynamicCompaction()
            .withFlushOnCommit()
            .build();
    assertEquals(new File(System.getProperty("user.dir"), "baz"), storage.directory());
    assertTrue(storage.dynamicCompaction());
    assertTrue(storage.isFlushOnCommit());
  }
}
