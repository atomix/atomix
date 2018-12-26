/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.counter;

import io.atomix.core.Atomix;
import io.atomix.core.value.AtomicValue;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Raft atomic counter test.
 */
public class RaftAtomicCounterTest extends AtomicCounterTest {
  @Override
  protected ProxyProtocol protocol() {
    return MultiRaftProtocol.builder()
        .withMaxRetries(5)
        .build();
  }

  @Test
  public void testDelete() throws Exception {
    Atomix client = atomix();

    AtomicCounter counter;
    counter = atomix().atomicCounterBuilder("test-delete")
        .withProtocol(protocol())
        .build();

    counter.set(1);
    assertEquals(1, counter.get());

    int count = client.getPrimitives(counter.type()).size();
    counter.delete();
    assertEquals(count - 1, client.getPrimitives(counter.type()).size());

    try {
      counter.get();
      fail();
    } catch (PrimitiveException.ClosedSession e) {
    }

    counter = atomix().atomicCounterBuilder("test-delete")
        .withProtocol(protocol())
        .build();
    assertFalse(client.getPrimitives(counter.type()).isEmpty());
    assertEquals(0, counter.get());
    counter.set(1);
    assertEquals(1, counter.get());
    assertEquals(count, client.getPrimitives(counter.type()).size());
  }
}
