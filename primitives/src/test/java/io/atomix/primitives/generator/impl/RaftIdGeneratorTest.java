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
package io.atomix.primitives.generator.impl;

import io.atomix.primitives.counter.impl.RaftCounter;
import io.atomix.primitives.counter.impl.RaftCounterService;
import io.atomix.primitives.impl.AbstractRaftPrimitiveTest;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.service.RaftService;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@code AtomixIdGenerator}.
 */
public class RaftIdGeneratorTest extends AbstractRaftPrimitiveTest<RaftCounter> {

  @Override
  protected RaftService createService() {
    return new RaftCounterService();
  }

  @Override
  protected RaftCounter createPrimitive(RaftProxy proxy) {
    return new RaftCounter(proxy);
  }

  /**
   * Tests generating IDs.
   */
  @Test
  public void testNextId() throws Throwable {
    RaftIdGenerator idGenerator1 = new RaftIdGenerator(newPrimitive("testNextId"));
    RaftIdGenerator idGenerator2 = new RaftIdGenerator(newPrimitive("testNextId"));

    CompletableFuture<Long> future11 = idGenerator1.nextId();
    CompletableFuture<Long> future12 = idGenerator1.nextId();
    CompletableFuture<Long> future13 = idGenerator1.nextId();
    assertEquals(Long.valueOf(1), future11.join());
    assertEquals(Long.valueOf(2), future12.join());
    assertEquals(Long.valueOf(3), future13.join());

    CompletableFuture<Long> future21 = idGenerator1.nextId();
    CompletableFuture<Long> future22 = idGenerator1.nextId();
    CompletableFuture<Long> future23 = idGenerator1.nextId();
    assertEquals(Long.valueOf(6), future23.join());
    assertEquals(Long.valueOf(5), future22.join());
    assertEquals(Long.valueOf(4), future21.join());

    CompletableFuture<Long> future31 = idGenerator2.nextId();
    CompletableFuture<Long> future32 = idGenerator2.nextId();
    CompletableFuture<Long> future33 = idGenerator2.nextId();
    assertEquals(Long.valueOf(1001), future31.join());
    assertEquals(Long.valueOf(1002), future32.join());
    assertEquals(Long.valueOf(1003), future33.join());
  }

  /**
   * Tests generating IDs.
   */
  @Test
  public void testNextIdBatchRollover() throws Throwable {
    RaftIdGenerator idGenerator1 = new RaftIdGenerator(newPrimitive("testNextIdBatchRollover"), 2);
    RaftIdGenerator idGenerator2 = new RaftIdGenerator(newPrimitive("testNextIdBatchRollover"), 2);

    CompletableFuture<Long> future11 = idGenerator1.nextId();
    CompletableFuture<Long> future12 = idGenerator1.nextId();
    CompletableFuture<Long> future13 = idGenerator1.nextId();
    assertEquals(Long.valueOf(1), future11.join());
    assertEquals(Long.valueOf(2), future12.join());
    assertEquals(Long.valueOf(3), future13.join());

    CompletableFuture<Long> future21 = idGenerator2.nextId();
    CompletableFuture<Long> future22 = idGenerator2.nextId();
    CompletableFuture<Long> future23 = idGenerator2.nextId();
    assertEquals(Long.valueOf(5), future21.join());
    assertEquals(Long.valueOf(6), future22.join());
    assertEquals(Long.valueOf(7), future23.join());

    CompletableFuture<Long> future14 = idGenerator1.nextId();
    CompletableFuture<Long> future15 = idGenerator1.nextId();
    CompletableFuture<Long> future16 = idGenerator1.nextId();
    assertEquals(Long.valueOf(4), future14.join());
    assertEquals(Long.valueOf(9), future15.join());
    assertEquals(Long.valueOf(10), future16.join());
  }
}
