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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Event log test.
 */
public class EventLogTest {
  @Test
  public void testEventLog() throws Exception {
    EventLog<String> eventLog = new EventLog<>();
    assertTrue(eventLog.register());
    assertFalse(eventLog.register());

    assertEquals(1, eventLog.newSession());
    assertEquals(2, eventLog.newSession());
    assertEquals(3, eventLog.newSession());

    eventLog.accept("a");
    eventLog.accept("b");
    eventLog.accept("c");

    CompletableFuture<String> globalFuture = eventLog.getGlobalSession().nextEvent();
    assertFalse(globalFuture.isDone());
    eventLog.accept("d");
    assertTrue(globalFuture.isDone());
    assertEquals("d", globalFuture.get());

    assertEquals("a", eventLog.getSession(1).nextEvent().get());
    assertEquals("a", eventLog.getSession(2).nextEvent().get());
    assertEquals("b", eventLog.getSession(1).nextEvent().get());
    assertEquals("c", eventLog.getSession(1).nextEvent().get());
    assertEquals("b", eventLog.getSession(2).nextEvent().get());
    assertEquals("a", eventLog.getSession(3).nextEvent().get());
    assertEquals("d", eventLog.getSession(1).nextEvent().get());

    CompletableFuture<String> nextEvent1 = eventLog.getGlobalSession().nextEvent();
    CompletableFuture<String> nextEvent2 = eventLog.getGlobalSession().nextEvent();

    assertFalse(nextEvent1.isDone());
    assertFalse(nextEvent2.isDone());

    CompletableFuture<String> nextSessionEvent1 = eventLog.getSession(1).nextEvent();
    CompletableFuture<String> nextSessionEvent2 = eventLog.getSession(1).nextEvent();

    assertFalse(nextSessionEvent1.isDone());
    assertFalse(nextSessionEvent2.isDone());

    eventLog.accept("e");

    assertTrue(nextEvent1.isDone());
    assertEquals("e", nextEvent1.get());
    assertFalse(nextEvent2.isDone());

    assertTrue(nextSessionEvent1.isDone());
    assertEquals("e", nextSessionEvent1.get());
    assertFalse(nextSessionEvent2.isDone());

    assertFalse(eventLog.unregister());
    eventLog.deleteGlobalSession();
    assertFalse(eventLog.unregister());
    eventLog.deleteSession(1);
    eventLog.deleteSession(2);
    eventLog.deleteSession(3);
    assertTrue(eventLog.unregister());
    assertFalse(eventLog.unregister());
  }
}
