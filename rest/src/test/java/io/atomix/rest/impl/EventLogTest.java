// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
