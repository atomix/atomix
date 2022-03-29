// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value;

import io.atomix.core.AbstractPrimitiveTest;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Raft atomic value test.
 */
public class AtomicValueTest extends AbstractPrimitiveTest {
  @Test
  public void testValue() throws Exception {
    AtomicValue<String> value = atomix().<String>atomicValueBuilder("test-value")
        .withProtocol(protocol())
        .build();
    assertNull(value.get());
    value.set("a");
    assertEquals("a", value.get());
    assertFalse(value.compareAndSet("b", "c"));
    assertTrue(value.compareAndSet("a", "b"));
    assertEquals("b", value.get());
    assertEquals("b", value.getAndSet("c"));
    assertEquals("c", value.get());
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
