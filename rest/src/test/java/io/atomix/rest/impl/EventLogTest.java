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
package io.atomix.rest.impl;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Event log test.
 */
public class EventLogTest {
  @Test
  public void testEventLog() throws Exception {
    EventLog<Consumer<String>, String> eventLog = new EventLog<>(l -> e -> l.addEvent(e));
    assertTrue(eventLog.open());
    assertFalse(eventLog.open());

    CompletableFuture<String> nextEvent = eventLog.nextEvent();
    assertFalse(nextEvent.isDone());

    eventLog.listener().accept("a");
    eventLog.listener().accept("b");

    assertTrue(nextEvent.isDone());
    assertEquals("a", nextEvent.get());
    assertEquals("b", eventLog.nextEvent().get());

    assertTrue(eventLog.close());
    assertFalse(eventLog.close());
  }
}
