/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.primitives.counter.impl;

import io.atomix.primitives.impl.AbstractRaftPrimitiveTest;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.service.RaftService;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RaftCounter}.
 */
public class RaftCounterTest extends AbstractRaftPrimitiveTest<RaftCounter> {
  @Override
  protected RaftService createService() {
    return new RaftCounterService();
  }

  @Override
  protected RaftCounter createPrimitive(RaftProxy proxy) {
    return new RaftCounter(proxy);
  }

  @Test
  public void testBasicOperations() throws Throwable {
    basicOperationsTest();
  }

  protected void basicOperationsTest() throws Throwable {
    RaftCounter along = newPrimitive("test-counter-basic-operations");
    assertEquals(0, along.get().join().longValue());
    assertEquals(1, along.incrementAndGet().join().longValue());
    along.set(100).join();
    assertEquals(100, along.get().join().longValue());
    assertEquals(100, along.getAndAdd(10).join().longValue());
    assertEquals(110, along.get().join().longValue());
    assertFalse(along.compareAndSet(109, 111).join());
    assertTrue(along.compareAndSet(110, 111).join());
    assertEquals(100, along.addAndGet(-11).join().longValue());
    assertEquals(100, along.getAndIncrement().join().longValue());
    assertEquals(101, along.get().join().longValue());
  }
}
