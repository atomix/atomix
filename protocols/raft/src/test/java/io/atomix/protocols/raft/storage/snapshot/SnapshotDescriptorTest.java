// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.snapshot;

import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Snapshot descriptor test.
 */
public class SnapshotDescriptorTest {

  @Test
  public void testSnapshotDescriptor() throws Exception {
    SnapshotDescriptor descriptor = SnapshotDescriptor.builder()
        .withIndex(2)
        .withTimestamp(3)
        .build();
    assertEquals(2, descriptor.index());
    assertEquals(3, descriptor.timestamp());
    assertEquals(1, descriptor.version());
  }

  @Test
  public void testCopySnapshotDescriptor() throws Exception {
    SnapshotDescriptor descriptor = SnapshotDescriptor.builder()
        .withIndex(2)
        .withTimestamp(3)
        .build();
    Buffer buffer = HeapBuffer.allocate(SnapshotDescriptor.BYTES);
    descriptor.copyTo(buffer);
    buffer.flip();
    descriptor = new SnapshotDescriptor(buffer);
    assertEquals(2, descriptor.index());
    assertEquals(3, descriptor.timestamp());
    assertEquals(1, descriptor.version());
  }

}
