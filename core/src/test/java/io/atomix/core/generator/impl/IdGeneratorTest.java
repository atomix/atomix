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
package io.atomix.core.generator.impl;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.generator.AsyncAtomicIdGenerator;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@code AtomixIdGenerator}.
 */
public class IdGeneratorTest extends AbstractPrimitiveTest {

  /**
   * Tests generating IDs.
   */
  @Test
  public void testNextId() throws Throwable {
    AsyncAtomicIdGenerator idGenerator1 = atomix().atomicIdGeneratorBuilder("testNextId").build().async();
    AsyncAtomicIdGenerator idGenerator2 = atomix().atomicIdGeneratorBuilder("testNextId").build().async();

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
    DelegatingAtomicIdGenerator idGenerator1 = new DelegatingAtomicIdGenerator(
        atomix().atomicCounterBuilder("testNextIdBatchRollover").build().async(), 2);
    DelegatingAtomicIdGenerator idGenerator2 = new DelegatingAtomicIdGenerator(
        atomix().atomicCounterBuilder("testNextIdBatchRollover").build().async(), 2);

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
