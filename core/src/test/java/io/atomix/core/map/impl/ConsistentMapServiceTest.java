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

import io.atomix.core.map.impl.ConsistentMapOperations.Get;
import io.atomix.core.map.impl.ConsistentMapOperations.Put;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.Scheduler;
import io.atomix.utils.time.Versioned;
import io.atomix.utils.time.WallClock;
import org.junit.Test;

import java.time.Duration;

import static io.atomix.core.map.impl.ConsistentMapOperations.GET;
import static io.atomix.core.map.impl.ConsistentMapOperations.PUT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Consistent map service test.
 */
public class ConsistentMapServiceTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testSnapshot() throws Exception {
    ConsistentMapService service = new TestConsistentMapService();

    service.put(new DefaultCommit<>(
        2,
        PUT,
        new Put("foo", "Hello world!".getBytes(), 1000),
        mock(Session.class),
        System.currentTimeMillis()));

    Buffer buffer = HeapBuffer.allocate();
    service.backup(buffer);

    service = new TestConsistentMapService();
    service.restore(buffer.flip());

    Versioned<byte[]> value = service.get(new DefaultCommit<>(
        2,
        GET,
        new Get("foo"),
        mock(Session.class),
        System.currentTimeMillis()));
    assertNotNull(value);
    assertArrayEquals("Hello world!".getBytes(), value.value());

    assertNotNull(service.entries().get("foo").timer);
  }

  private static class TestConsistentMapService extends ConsistentMapService {
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
