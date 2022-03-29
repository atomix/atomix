// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.tree.impl;

import io.atomix.core.tree.DocumentPath;
import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.time.Versioned;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Document tree service test.
 */
public class DefaultAtomicDocumentTreeServiceTest {
  @Test
  public void testSnapshot() throws Exception {
    DefaultDocumentTreeService service = new DefaultDocumentTreeService();
    service.set(DocumentPath.from("/foo"), "Hello world!".getBytes());

    Buffer buffer = HeapBuffer.allocate();
    service.backup(new DefaultBackupOutput(buffer, service.serializer()));

    service = new DefaultDocumentTreeService();
    service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

    Versioned<byte[]> value = service.get(DocumentPath.from("/foo"));
    assertNotNull(value);
    assertArrayEquals("Hello world!".getBytes(), value.value());
  }
}
