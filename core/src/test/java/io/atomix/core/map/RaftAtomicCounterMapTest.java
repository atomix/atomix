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
package io.atomix.core.map;

import io.atomix.core.Atomix;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Raft atomic counter map test.
 */
public class RaftAtomicCounterMapTest extends AtomicCounterMapTest {
  @Override
  protected ProxyProtocol protocol() {
    return MultiRaftProtocol.builder()
        .withMaxRetries(5)
        .build();
  }

  @Test
  public void testDelete() throws Exception {
    Atomix client = atomix();

    AtomicCounterMap<String> counterMap;
    counterMap = atomix().<String>atomicCounterMapBuilder("test-delete")
        .withProtocol(protocol())
        .build();
    assertFalse(client.getPrimitives(counterMap.type()).isEmpty());
    counterMap.delete();
    assertTrue(client.getPrimitives(counterMap.type()).isEmpty());

    try {
      counterMap.get("foo");
      fail();
    } catch (PrimitiveException.ClosedSession e) {
    }

    counterMap = atomix().<String>atomicCounterMapBuilder("test-delete")
        .withProtocol(protocol())
        .build();
    assertFalse(client.getPrimitives(counterMap.type()).isEmpty());
  }
}
