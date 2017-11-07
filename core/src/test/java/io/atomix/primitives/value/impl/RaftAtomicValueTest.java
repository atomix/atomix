/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitives.value.impl;

import io.atomix.primitives.impl.AbstractRaftPrimitiveTest;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.service.RaftService;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Raft atomic value test.
 */
public class RaftValueTest extends AbstractRaftPrimitiveTest<RaftValue> {
  @Override
  protected RaftService createService() {
    return new RaftValueService();
  }

  @Override
  protected RaftValue createPrimitive(RaftProxy proxy) {
    return new RaftValue(proxy);
  }

  @Test
  public void testValue() throws Exception {
    byte[] bytes1 = "a".getBytes();
    byte[] bytes2 = "b".getBytes();
    byte[] bytes3 = "c".getBytes();

    RaftValue value = newPrimitive("test-value");
    assertEquals(0, value.get().join().length);
    value.set(bytes1).join();
    assertArrayEquals(bytes1, value.get().join());
    assertFalse(value.compareAndSet(bytes2, bytes3).join());
    assertTrue(value.compareAndSet(bytes1, bytes2).join());
    assertArrayEquals(bytes2, value.get().join());
    assertArrayEquals(bytes2, value.getAndSet(bytes3).join());
    assertArrayEquals(bytes3, value.get().join());
  }
}
