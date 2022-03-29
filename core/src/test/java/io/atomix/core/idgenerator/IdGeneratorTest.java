// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.idgenerator;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.idgenerator.impl.DelegatingAtomicIdGenerator;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
    AtomicIdGenerator idGenerator1 = atomix().atomicIdGeneratorBuilder("testNextId")
        .withProtocol(protocol())
        .build();
    AtomicIdGenerator idGenerator2 = atomix().atomicIdGeneratorBuilder("testNextId")
        .withProtocol(protocol())
        .build();

    assertEquals(1, idGenerator1.nextId());
    assertEquals(2, idGenerator1.nextId());
    assertEquals(3, idGenerator1.nextId());

    CompletableFuture<Long> future21 = idGenerator1.async().nextId();
    CompletableFuture<Long> future22 = idGenerator1.async().nextId();
    CompletableFuture<Long> future23 = idGenerator1.async().nextId();
    assertEquals(Long.valueOf(6), future23.get(30, TimeUnit.SECONDS));
    assertEquals(Long.valueOf(5), future22.get(30, TimeUnit.SECONDS));
    assertEquals(Long.valueOf(4), future21.get(30, TimeUnit.SECONDS));

    assertEquals(1001, idGenerator2.nextId());
    assertEquals(1002, idGenerator2.nextId());
    assertEquals(1003, idGenerator2.nextId());
  }

  /**
   * Tests generating IDs.
   */
  @Test
  public void testNextIdBatchRollover() throws Throwable {
    DelegatingAtomicIdGenerator idGenerator1 = new DelegatingAtomicIdGenerator(
        atomix().atomicCounterBuilder("testNextIdBatchRollover")
            .withProtocol(protocol())
            .build()
            .async(), 2);
    DelegatingAtomicIdGenerator idGenerator2 = new DelegatingAtomicIdGenerator(
        atomix().atomicCounterBuilder("testNextIdBatchRollover")
            .withProtocol(protocol())
            .build()
            .async(), 2);

    CompletableFuture<Long> future11 = idGenerator1.nextId();
    CompletableFuture<Long> future12 = idGenerator1.nextId();
    CompletableFuture<Long> future13 = idGenerator1.nextId();
    assertEquals(Long.valueOf(1), future11.get(30, TimeUnit.SECONDS));
    assertEquals(Long.valueOf(2), future12.get(30, TimeUnit.SECONDS));
    assertEquals(Long.valueOf(3), future13.get(30, TimeUnit.SECONDS));

    CompletableFuture<Long> future21 = idGenerator2.nextId();
    CompletableFuture<Long> future22 = idGenerator2.nextId();
    CompletableFuture<Long> future23 = idGenerator2.nextId();
    assertEquals(Long.valueOf(5), future21.get(30, TimeUnit.SECONDS));
    assertEquals(Long.valueOf(6), future22.get(30, TimeUnit.SECONDS));
    assertEquals(Long.valueOf(7), future23.get(30, TimeUnit.SECONDS));

    CompletableFuture<Long> future14 = idGenerator1.nextId();
    CompletableFuture<Long> future15 = idGenerator1.nextId();
    CompletableFuture<Long> future16 = idGenerator1.nextId();
    assertEquals(Long.valueOf(4), future14.get(30, TimeUnit.SECONDS));
    assertEquals(Long.valueOf(9), future15.get(30, TimeUnit.SECONDS));
    assertEquals(Long.valueOf(10), future16.get(30, TimeUnit.SECONDS));
  }
}
