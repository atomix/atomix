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
package io.atomix.core.map.impl;

import io.atomix.core.map.AtomicMapType;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.Scheduler;
import io.atomix.utils.time.Versioned;
import io.atomix.utils.time.WallClock;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Consistent map service test.
 */
public class DefaultAtomicMapServiceTest {

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

    AbstractAtomicMapService service = new TestAtomicMapService();
    service.init(context);

    service.put("foo", "Hello world!".getBytes());

    Buffer buffer = HeapBuffer.allocate();
    service.backup(new DefaultBackupOutput(buffer, service.serializer()));

    service = new TestAtomicMapService();
    service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

    Versioned<byte[]> value = service.get("foo");
    assertNotNull(value);
    assertArrayEquals("Hello world!".getBytes(), value.value());
  }

  private static class TestAtomicMapService extends AbstractAtomicMapService {
    TestAtomicMapService() {
      super(AtomicMapType.instance());
    }

    @Override
    protected Scheduler getScheduler() {
      return new Scheduler() {
        @Override
        public Scheduled schedule(Duration delay, Runnable callback) {
          return mock(Scheduled.class);
        }

        @Override
        public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
          return mock(Scheduled.class);
        }
      };
    }

    @Override
    protected WallClock getWallClock() {
      return new WallClock();
    }
  }
}
