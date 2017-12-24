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
package io.atomix.core.multimap.impl;

import io.atomix.core.multimap.impl.ConsistentSetMultimapService;
import io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.Get;
import io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.Put;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.time.Versioned;
import io.atomix.utils.Match;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.GET;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.PUT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Consistent set multimap service test.
 */
public class ConsistentSetMultimapServiceTest {
  @Test
  @SuppressWarnings("unchecked")
  public void testSnapshot() throws Exception {
    ConsistentSetMultimapService service = new ConsistentSetMultimapService();
    service.put(new DefaultCommit<>(
        2,
        PUT,
        new Put(
            "foo", Arrays.asList("Hello world!".getBytes()), Match.ANY),
        mock(Session.class),
        System.currentTimeMillis()));

    Buffer buffer = HeapBuffer.allocate();
    service.backup(buffer);

    service = new ConsistentSetMultimapService();
    service.restore(buffer.flip());

    Versioned<Collection<? extends byte[]>> value = service.get(new DefaultCommit<>(
        2,
        GET,
        new Get("foo"),
        mock(Session.class),
        System.currentTimeMillis()));
    assertNotNull(value);
    assertEquals(1, value.value().size());
    assertArrayEquals("Hello world!".getBytes(), value.value().iterator().next());
  }
}
