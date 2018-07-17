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
import io.atomix.primitive.protocol.value.ValueProtocol;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Distributed value test.
 */
public abstract class DistributedValueTest extends AbstractPrimitiveTest {
  @Test
  public void testValue() throws Exception {
    DistributedValue<String> value1 = atomix().<String>valueBuilder("test-value")
        .withProtocol((ValueProtocol) protocol())
        .build();
    DistributedValue<String> value2 = atomix().<String>valueBuilder("test-value")
        .withProtocol((ValueProtocol) protocol())
        .build();

    BlockingValueListener<String> listener = new BlockingValueListener<>();
    value2.addListener(listener);

    assertNull(value1.get());
    assertNull(value2.get());

    value1.set("foo");

    ValueEvent<String> event;
    event = listener.nextEvent();
    assertEquals(ValueEvent.Type.UPDATE, event.type());
    assertEquals("foo", event.newValue());

    assertEquals("foo", value1.get());
    assertEquals("foo", value2.get());

    assertEquals("foo", value1.getAndSet("bar"));
    assertEquals("bar", value1.get());

    event = listener.nextEvent();
    assertEquals(ValueEvent.Type.UPDATE, event.type());
    assertEquals("bar", event.newValue());
    assertEquals("bar", value2.get());
  }

  private static class BlockingValueListener<T> implements ValueEventListener<T> {
    private final BlockingQueue<ValueEvent<T>> events = new LinkedBlockingQueue<>();

    @Override
    public void event(ValueEvent<T> event) {
      events.add(event);
    }

    /**
     * Returns the next event.
     *
     * @return the next event
     */
    ValueEvent<T> nextEvent() {
      try {
        return events.poll(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        return null;
      }
    }
  }
}
