// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter.impl;

import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Counter service test.
 */
public class DefaultAtomicCounterServiceTest {
  @Test
  public void testSnapshot() throws Exception {
    DefaultAtomicCounterService service = new DefaultAtomicCounterService();
    service.set(1);

    Buffer buffer = HeapBuffer.allocate();
    service.backup(new DefaultBackupOutput(buffer, service.serializer()));

    service = new DefaultAtomicCounterService();
    service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

    long value = service.get();
    assertEquals(1, value);
  }
}
