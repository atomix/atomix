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
package io.atomix.primitives.leadership.impl;

import io.atomix.primitives.impl.AbstractRaftPrimitiveTest;
import io.atomix.primitives.leadership.Leadership;
import io.atomix.primitives.leadership.LeadershipEvent;
import io.atomix.primitives.leadership.LeadershipEventListener;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.service.RaftService;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RaftLeaderElector}.
 */
public class RaftLeaderElectorTest extends AbstractRaftPrimitiveTest<RaftLeaderElector> {

  byte[] node1 = "node1".getBytes();
  byte[] node2 = "node2".getBytes();
  byte[] node3 = "node3".getBytes();

  @Override
  protected RaftService createService() {
    return new RaftLeaderElectorService();
  }

  @Override
  protected RaftLeaderElector createPrimitive(RaftProxy proxy) {
    return new RaftLeaderElector(proxy);
  }

  @Test
  public void testRun() throws Throwable {
    RaftLeaderElector elector1 = newPrimitive("test-elector-run");
    elector1.run(node1).thenAccept(result -> {
      assertArrayEquals(node1, result.leader().id());
      assertEquals(1, result.leader().term());
      assertEquals(1, result.candidates().size());
      assertArrayEquals(node1, result.candidates().get(0));
    }).join();

    RaftLeaderElector elector2 = newPrimitive("test-elector-run");
    elector2.run(node2).thenAccept(result -> {
      assertArrayEquals(node1, result.leader().id());
      assertEquals(1, result.leader().term());
      assertEquals(2, result.candidates().size());
      assertArrayEquals(node1, result.candidates().get(0));
      assertArrayEquals(node2, result.candidates().get(1));
    }).join();
  }

  @Test
  public void testWithdraw() throws Throwable {
    RaftLeaderElector elector1 = newPrimitive("test-elector-withdraw");
    elector1.run(node1).join();
    RaftLeaderElector elector2 = newPrimitive("test-elector-withdraw");
    elector2.run(node2).join();

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener(listener1).join();

    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener(listener2).join();

    elector1.withdraw().join();

    listener1.nextEvent().thenAccept(result -> {
      assertArrayEquals(node2, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().leader().term());
      assertEquals(1, result.newLeadership().candidates().size());
      assertArrayEquals(node2, result.newLeadership().candidates().get(0));
    }).join();

    listener2.nextEvent().thenAccept(result -> {
      assertArrayEquals(node2, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().leader().term());
      assertEquals(1, result.newLeadership().candidates().size());
      assertArrayEquals(node2, result.newLeadership().candidates().get(0));
    }).join();

    Leadership<byte[]> leadership1 = elector1.getLeadership().join();
    assertArrayEquals(node2, leadership1.leader().id());
    assertEquals(1, leadership1.candidates().size());

    Leadership<byte[]> leadership2 = elector2.getLeadership().join();
    assertArrayEquals(node2, leadership2.leader().id());
    assertEquals(1, leadership2.candidates().size());
  }

  @Test
  public void testAnoint() throws Throwable {
    RaftLeaderElector elector1 = newPrimitive("test-elector-anoint");
    RaftLeaderElector elector2 = newPrimitive("test-elector-anoint");
    RaftLeaderElector elector3 = newPrimitive("test-elector-anoint");
    elector1.run(node1).join();
    elector2.run(node2).join();

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener(listener1).join();
    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener(listener2);
    LeaderEventListener listener3 = new LeaderEventListener();
    elector3.addListener(listener3).join();

    elector3.anoint(node3).thenAccept(result -> {
      assertFalse(result);
    }).join();
    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());
    assertFalse(listener3.hasEvent());

    elector3.anoint(node2).thenAccept(result -> {
      assertTrue(result);
    }).join();

    listener1.nextEvent().thenAccept(result -> {
      assertArrayEquals(node2, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().candidates().size());
      assertArrayEquals(node1, result.newLeadership().candidates().get(0));
      assertArrayEquals(node2, result.newLeadership().candidates().get(1));
    }).join();
    listener2.nextEvent().thenAccept(result -> {
      assertArrayEquals(node2, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().candidates().size());
      assertArrayEquals(node1, result.newLeadership().candidates().get(0));
      assertArrayEquals(node2, result.newLeadership().candidates().get(1));
    }).join();
    listener3.nextEvent().thenAccept(result -> {
      assertArrayEquals(node2, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().candidates().size());
      assertArrayEquals(node1, result.newLeadership().candidates().get(0));
      assertArrayEquals(node2, result.newLeadership().candidates().get(1));
    }).join();
  }

  @Test
  public void testPromote() throws Throwable {
    RaftLeaderElector elector1 = newPrimitive("test-elector-promote");
    RaftLeaderElector elector2 = newPrimitive("test-elector-promote");
    RaftLeaderElector elector3 = newPrimitive("test-elector-promote");
    elector1.run(node1).join();
    elector2.run(node2).join();

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener(listener1).join();
    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener(listener2).join();
    LeaderEventListener listener3 = new LeaderEventListener();
    elector3.addListener(listener3).join();

    elector3.promote(node3).thenAccept(result -> {
      assertFalse(result);
    }).join();

    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());
    assertFalse(listener3.hasEvent());

    elector3.run(node3).join();

    listener1.nextEvent().thenAccept(result -> {
      assertArrayEquals(node3, result.newLeadership().candidates().get(2));
    }).join();
    listener2.nextEvent().thenAccept(result -> {
      assertArrayEquals(node3, result.newLeadership().candidates().get(2));
    }).join();
    listener3.nextEvent().thenAccept(result -> {
      assertArrayEquals(node3, result.newLeadership().candidates().get(2));
    }).join();

    elector3.promote(node3).thenAccept(result -> {
      assertTrue(result);
    }).join();

    listener1.nextEvent().thenAccept(result -> {
      assertArrayEquals(node3, result.newLeadership().candidates().get(0));
    }).join();
    listener2.nextEvent().thenAccept(result -> {
      assertArrayEquals(node3, result.newLeadership().candidates().get(0));
    }).join();
    listener3.nextEvent().thenAccept(result -> {
      assertArrayEquals(node3, result.newLeadership().candidates().get(0));
    }).join();
  }

  @Test
  public void testLeaderSessionClose() throws Throwable {
    RaftLeaderElector elector1 = newPrimitive("test-elector-leader-session-close");
    elector1.run(node1).join();
    RaftLeaderElector elector2 = newPrimitive("test-elector-leader-session-close");
    LeaderEventListener listener = new LeaderEventListener();
    elector2.run(node2).join();
    elector2.addListener(listener).join();
    elector1.close();
    listener.nextEvent().thenAccept(result -> {
      assertArrayEquals(node2, result.newLeadership().leader().id());
      assertEquals(1, result.newLeadership().candidates().size());
      assertArrayEquals(node2, result.newLeadership().candidates().get(0));
    }).join();
  }

  @Test
  public void testNonLeaderSessionClose() throws Throwable {
    RaftLeaderElector elector1 = newPrimitive("test-elector-non-leader-session-close");
    elector1.run(node1).join();
    RaftLeaderElector elector2 = newPrimitive("test-elector-non-leader-session-close");
    LeaderEventListener listener = new LeaderEventListener();
    elector2.run(node2).join();
    elector1.addListener(listener).join();
    elector2.close().join();
    listener.nextEvent().thenAccept(result -> {
      assertArrayEquals(node1, result.newLeadership().leader().id());
      assertEquals(1, result.newLeadership().candidates().size());
      assertArrayEquals(node1, result.newLeadership().candidates().get(0));
    }).join();
  }

  @Test
  public void testQueries() throws Throwable {
    RaftLeaderElector elector1 = newPrimitive("test-elector-query");
    RaftLeaderElector elector2 = newPrimitive("test-elector-query");
    elector1.run(node1).join();
    elector2.run(node2).join();
    elector2.run(node2).join();
    elector1.getLeadership().thenAccept(result -> {
      assertArrayEquals(node1, result.leader().id());
      assertArrayEquals(node1, result.candidates().get(0));
      assertArrayEquals(node2, result.candidates().get(1));
    }).join();
    elector2.getLeadership().thenAccept(result -> {
      assertArrayEquals(node1, result.leader().id());
      assertArrayEquals(node1, result.candidates().get(0));
      assertArrayEquals(node2, result.candidates().get(1));
    }).join();
  }

  private static class LeaderEventListener implements LeadershipEventListener<byte[]> {
    Queue<LeadershipEvent<byte[]>> eventQueue = new LinkedList<>();
    CompletableFuture<LeadershipEvent<byte[]>> pendingFuture;

    @Override
    public void onEvent(LeadershipEvent<byte[]> event) {
      synchronized (this) {
        if (pendingFuture != null) {
          pendingFuture.complete(event);
          pendingFuture = null;
        } else {
          eventQueue.add(event);
        }
      }
    }

    public boolean hasEvent() {
      return !eventQueue.isEmpty();
    }

    public void clearEvents() {
      eventQueue.clear();
    }

    public CompletableFuture<LeadershipEvent<byte[]>> nextEvent() {
      synchronized (this) {
        if (eventQueue.isEmpty()) {
          if (pendingFuture == null) {
            pendingFuture = new CompletableFuture<>();
          }
          return pendingFuture;
        } else {
          return CompletableFuture.completedFuture(eventQueue.poll());
        }
      }
    }
  }
}
