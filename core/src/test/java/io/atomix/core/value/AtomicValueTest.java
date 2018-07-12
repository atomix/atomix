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
package io.atomix.core.value;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.primitive.protocol.ProxyProtocol;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Raft atomic value test.
 */
public abstract class AtomicValueTest extends AbstractPrimitiveTest<ProxyProtocol> {
  @Test
  public void testValue() throws Exception {
    AsyncAtomicValue<String> value = atomix().<String>atomicValueBuilder("test-value")
        .withProtocol(protocol())
        .build()
        .async();
    assertNull(value.get().get(30, TimeUnit.SECONDS));
    value.set("a").get(30, TimeUnit.SECONDS);
    assertEquals("a", value.get().get(30, TimeUnit.SECONDS));
    assertFalse(value.compareAndSet("b", "c").get(30, TimeUnit.SECONDS));
    assertTrue(value.compareAndSet("a", "b").get(30, TimeUnit.SECONDS));
    assertEquals("b", value.get().get(30, TimeUnit.SECONDS));
    assertEquals("b", value.getAndSet("c").get(30, TimeUnit.SECONDS));
    assertEquals("c", value.get().get(30, TimeUnit.SECONDS));
  }

  @Test
  public void testEvents() throws Exception {
    AtomicValue<String> value1 = atomix().<String>atomicValueBuilder("test-value-events")
        .withProtocol(protocol())
        .build();
    AtomicValue<String> value2 = atomix().<String>atomicValueBuilder("test-value-events")
        .withProtocol(protocol())
        .build();

    BlockingAtomicValueListener<String> listener1 = new BlockingAtomicValueListener<>();
    BlockingAtomicValueListener<String> listener2 = new BlockingAtomicValueListener<>();

    value2.addListener(listener2);

    value1.set("Hello world!");
    assertEquals("Hello world!", listener2.nextEvent().newValue());

    value1.set("Hello world again!");
    assertEquals("Hello world again!", listener2.nextEvent().newValue());

    value1.addListener(listener1);

    value2.set("Hello world back!");
    assertEquals("Hello world back!", listener1.nextEvent().newValue());
    assertEquals("Hello world back!", listener2.nextEvent().newValue());
  }

  private static class BlockingAtomicValueListener<T> implements AtomicValueEventListener<T> {
    private final BlockingQueue<AtomicValueEvent<T>> events = new LinkedBlockingQueue<>();

    @Override
    public void event(AtomicValueEvent<T> event) {
      events.add(event);
    }

    /**
     * Returns the next event.
     *
     * @return the next event
     */
    AtomicValueEvent<T> nextEvent() {
      try {
        return events.take();
      } catch (InterruptedException e) {
        return null;
      }
    }
  }
}
