// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.counter.impl.AtomicCounterProxy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AtomicCounterProxy}.
 */
public class AtomicCounterTest extends AbstractPrimitiveTest {
  @Test
  public void testBasicOperations() throws Throwable {
    AtomicCounter along = atomix().atomicCounterBuilder("test-counter-basic-operations")
        .withProtocol(protocol())
        .build();
    assertEquals(0, along.get());
    assertEquals(1, along.incrementAndGet());
    along.set(100);
    assertEquals(100, along.get());
    assertEquals(100, along.getAndAdd(10));
    assertEquals(110, along.get());
    assertFalse(along.compareAndSet(109, 111));
    assertTrue(along.compareAndSet(110, 111));
    assertEquals(100, along.addAndGet(-11));
    assertEquals(100, along.getAndIncrement());
    assertEquals(101, along.get());
    assertEquals(100, along.decrementAndGet());
    assertEquals(100, along.getAndDecrement());
    assertEquals(99, along.get());
  }
}
