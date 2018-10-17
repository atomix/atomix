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
