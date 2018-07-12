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
import io.atomix.primitive.protocol.ProxyProtocol;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AtomicCounterProxy}.
 */
public abstract class AtomicCounterTest extends AbstractPrimitiveTest<ProxyProtocol> {
  @Test
  public void testBasicOperations() throws Throwable {
    AsyncAtomicCounter along = atomix().atomicCounterBuilder("test-counter-basic-operations")
        .withProtocol(protocol())
        .build()
        .async();
    assertEquals(0, along.get().get(30, TimeUnit.SECONDS).longValue());
    assertEquals(1, along.incrementAndGet().get(30, TimeUnit.SECONDS).longValue());
    along.set(100).get(30, TimeUnit.SECONDS);
    assertEquals(100, along.get().get(30, TimeUnit.SECONDS).longValue());
    assertEquals(100, along.getAndAdd(10).get(30, TimeUnit.SECONDS).longValue());
    assertEquals(110, along.get().get(30, TimeUnit.SECONDS).longValue());
    assertFalse(along.compareAndSet(109, 111).get(30, TimeUnit.SECONDS));
    assertTrue(along.compareAndSet(110, 111).get(30, TimeUnit.SECONDS));
    assertEquals(100, along.addAndGet(-11).get(30, TimeUnit.SECONDS).longValue());
    assertEquals(100, along.getAndIncrement().get(30, TimeUnit.SECONDS).longValue());
    assertEquals(101, along.get().get(30, TimeUnit.SECONDS).longValue());
    assertEquals(100, along.decrementAndGet().get(30, TimeUnit.SECONDS).longValue());
    assertEquals(100, along.getAndDecrement().get(30, TimeUnit.SECONDS).longValue());
    assertEquals(99, along.get().get(30, TimeUnit.SECONDS).longValue());
  }
}
