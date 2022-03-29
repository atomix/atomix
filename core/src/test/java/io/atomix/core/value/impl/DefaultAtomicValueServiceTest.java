// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value.impl;

import io.atomix.core.map.AtomicMapType;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.time.WallClock;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Atomic value service test.
 */
public class DefaultAtomicValueServiceTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testSnapshot() throws Exception {
    ServiceContext context = mock(ServiceContext.class);
    when(context.serviceType()).thenReturn(AtomicMapType.instance());
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));
    when(context.wallClock()).thenReturn(new WallClock());

    Session session = mock(Session.class);
    when(session.sessionId()).thenReturn(SessionId.from(1));

    DefaultAtomicValueService service = new DefaultAtomicValueService();
    service.init(context);

    assertNull(service.get());

    Buffer buffer = HeapBuffer.allocate();
    service.backup(new DefaultBackupOutput(buffer, service.serializer()));

    assertNull(service.get());

    service = new DefaultAtomicValueService();
    service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

    assertNull(service.get());

    service.set("Hello world!".getBytes());
    assertArrayEquals("Hello world!".getBytes(), service.get());

    buffer = HeapBuffer.allocate();
    service.backup(new DefaultBackupOutput(buffer, service.serializer()));

    assertArrayEquals("Hello world!".getBytes(), service.get());

    service = new DefaultAtomicValueService();
    service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

    assertArrayEquals("Hello world!".getBytes(), service.get());

    service.set(null);
    assertNull(service.get());
  }
}
