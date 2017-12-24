/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.queue.impl;

import io.atomix.core.PrimitiveTypes;
import io.atomix.core.queue.Task;
import io.atomix.core.queue.impl.WorkQueueService;
import io.atomix.core.queue.impl.WorkQueueOperations.Add;
import io.atomix.core.queue.impl.WorkQueueOperations.Take;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static io.atomix.core.queue.impl.WorkQueueOperations.ADD;
import static io.atomix.core.queue.impl.WorkQueueOperations.TAKE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Work queue service test.
 */
public class WorkQueueServiceTest {
  @Test
  public void testSnapshot() throws Exception {
    ServiceContext context = mock(ServiceContext.class);
    when(context.serviceType()).thenReturn(PrimitiveTypes.workQueue());
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));

    Session session = mock(Session.class);
    when(session.sessionId()).thenReturn(SessionId.from(1));

    WorkQueueService service = new WorkQueueService();
    service.init(context);

    service.add(new DefaultCommit<>(
        2,
        ADD,
        new Add(Arrays.asList("Hello world!".getBytes())),
        session,
        System.currentTimeMillis()));

    Buffer buffer = HeapBuffer.allocate();
    service.backup(buffer);

    service = new WorkQueueService();
    service.init(context);
    service.restore(buffer.flip());

    Collection<Task<byte[]>> value = service.take(new DefaultCommit<>(
        2,
        TAKE,
        new Take(1),
        session,
        System.currentTimeMillis()));
    assertNotNull(value);
    assertEquals(1, value.size());
    assertArrayEquals("Hello world!".getBytes(), value.iterator().next().payload());
  }
}
