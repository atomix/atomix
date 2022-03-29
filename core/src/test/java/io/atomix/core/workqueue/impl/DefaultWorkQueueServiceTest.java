// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.workqueue.impl;

import io.atomix.core.workqueue.Task;
import io.atomix.core.workqueue.WorkQueueType;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Work queue service test.
 */
public class DefaultWorkQueueServiceTest {
  @Test
  public void testSnapshot() throws Exception {
    ServiceContext context = mock(ServiceContext.class);
    when(context.serviceType()).thenReturn(WorkQueueType.instance());
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));

    Session session = mock(Session.class);
    when(session.sessionId()).thenReturn(SessionId.from(1));
    when(context.currentSession()).thenReturn(session);

    DefaultWorkQueueService service = new DefaultWorkQueueService();
    service.init(context);
    service.register(session);

    service.add(Arrays.asList("Hello world!".getBytes()));

    Buffer buffer = HeapBuffer.allocate();
    service.backup(new DefaultBackupOutput(buffer, service.serializer()));

    service = new DefaultWorkQueueService();
    service.init(context);
    service.register(session);
    service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

    Collection<Task<byte[]>> value = service.take(1);
    assertNotNull(value);
    assertEquals(1, value.size());
    assertArrayEquals("Hello world!".getBytes(), value.iterator().next().payload());
  }
}
