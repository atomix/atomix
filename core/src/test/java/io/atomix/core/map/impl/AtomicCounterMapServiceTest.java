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

import io.atomix.core.map.impl.AtomicCounterMapService;
import io.atomix.core.map.impl.AtomicCounterMapOperations.Get;
import io.atomix.core.map.impl.AtomicCounterMapOperations.Put;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import org.junit.Test;

import static io.atomix.core.map.impl.AtomicCounterMapOperations.GET;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.PUT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Atomic counter map service test.
 */
public class AtomicCounterMapServiceTest {
  @Test
  public void testSnapshot() throws Exception {
    AtomicCounterMapService service = new AtomicCounterMapService();
    service.put(new DefaultCommit<>(
        2,
        PUT,
        new Put("foo", 1),
        mock(Session.class),
        System.currentTimeMillis()));

    Buffer buffer = HeapBuffer.allocate();
    service.backup(buffer);

    service = new AtomicCounterMapService();
    service.restore(buffer.flip());

    long value = service.get(new DefaultCommit<>(
        2,
        GET,
        new Get("foo"),
        mock(Session.class),
        System.currentTimeMillis()));
    assertEquals(1, value);
  }
}
