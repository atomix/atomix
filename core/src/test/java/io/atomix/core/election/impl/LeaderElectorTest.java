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
package io.atomix.core.election.impl;

import io.atomix.cluster.NodeId;
import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.election.AsyncLeaderElector;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.election.LeadershipEventListener;

import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Leader elector test.
 */
public class LeaderElectorTest extends AbstractPrimitiveTest {

  NodeId node1 = new NodeId("4");
  NodeId node2 = new NodeId("5");
  NodeId node3 = new NodeId("6");

  @Test
  public void testRun() throws Throwable {
    AsyncLeaderElector<NodeId> elector1 = atomix().<NodeId>leaderElectorBuilder("test-elector-run").build().async();
    elector1.run("foo", node1).thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(1, result.leader().term());
      assertEquals(1, result.candidates().size());
      assertEquals(node1, result.candidates().get(0));
    }).join();

    elector1.run("bar", node1).thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(1, result.leader().term());
      assertEquals(1, result.candidates().size());
      assertEquals(node1, result.candidates().get(0));
    }).join();

    AsyncLeaderElector<NodeId> elector2 = atomix().<NodeId>leaderElectorBuilder("test-elector-run").build().async();
    elector2.run("bar", node2).thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(1, result.leader().term());
      assertEquals(2, result.candidates().size());
      assertEquals(node1, result.candidates().get(0));
      assertEquals(node2, result.candidates().get(1));
    }).join();
  }

  @Test
  public void testWithdraw() throws Throwable {
    AsyncLeaderElector<NodeId> elector1 = atomix().<NodeId>leaderElectorBuilder("test-elector-withdraw").build().async();
    elector1.run("foo", node1).join();
    AsyncLeaderElector<NodeId> elector2 = atomix().<NodeId>leaderElectorBuilder("test-elector-withdraw").build().async();
    elector2.run("foo", node2).join();

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener(listener1).join();

    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener(listener2).join();

    elector1.withdraw("foo", node1).join();

    listener1.nextEvent().thenAccept(result -> {
      assertEquals(node2, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().leader().term());
      assertEquals(1, result.newLeadership().candidates().size());
      assertEquals(node2, result.newLeadership().candidates().get(0));
    }).join();

    listener2.nextEvent().thenAccept(result -> {
      assertEquals(node2, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().leader().term());
      assertEquals(1, result.newLeadership().candidates().size());
      assertEquals(node2, result.newLeadership().candidates().get(0));
    }).join();

    Leadership leadership1 = elector1.getLeadership("foo").join();
    assertEquals(node2, leadership1.leader().id());
    assertEquals(1, leadership1.candidates().size());

    Leadership leadership2 = elector2.getLeadership("foo").join();
    assertEquals(node2, leadership2.leader().id());
    assertEquals(1, leadership2.candidates().size());
  }

  @Test
  public void testAnoint() throws Throwable {
    AsyncLeaderElector<NodeId> elector1 = atomix().<NodeId>leaderElectorBuilder("test-elector-anoint").build().async();
    AsyncLeaderElector<NodeId> elector2 = atomix().<NodeId>leaderElectorBuilder("test-elector-anoint").build().async();
    AsyncLeaderElector<NodeId> elector3 = atomix().<NodeId>leaderElectorBuilder("test-elector-anoint").build().async();
    elector1.run("foo", node1).join();
    elector2.run("foo", node2).join();

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener(listener1).join();
    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener(listener2);
    LeaderEventListener listener3 = new LeaderEventListener();
    elector3.addListener(listener3).join();

    elector3.anoint("foo", node3).thenAccept(result -> {
      assertFalse(result);
    }).join();
    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());
    assertFalse(listener3.hasEvent());

    elector3.anoint("foo", node2).thenAccept(result -> {
      assertTrue(result);
    }).join();

    listener1.nextEvent().thenAccept(result -> {
      assertEquals(node2, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().candidates().size());
      assertEquals(node1, result.newLeadership().candidates().get(0));
      assertEquals(node2, result.newLeadership().candidates().get(1));
    }).join();
    listener2.nextEvent().thenAccept(result -> {
      assertEquals(node2, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().candidates().size());
      assertEquals(node1, result.newLeadership().candidates().get(0));
      assertEquals(node2, result.newLeadership().candidates().get(1));
    }).join();
    listener3.nextEvent().thenAccept(result -> {
      assertEquals(node2, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().candidates().size());
      assertEquals(node1, result.newLeadership().candidates().get(0));
      assertEquals(node2, result.newLeadership().candidates().get(1));
    }).join();
  }

  @Test
  public void testPromote() throws Throwable {
    AsyncLeaderElector<NodeId> elector1 = atomix().<NodeId>leaderElectorBuilder("test-elector-promote").build().async();
    AsyncLeaderElector<NodeId> elector2 = atomix().<NodeId>leaderElectorBuilder("test-elector-promote").build().async();
    AsyncLeaderElector<NodeId> elector3 = atomix().<NodeId>leaderElectorBuilder("test-elector-promote").build().async();
    elector1.run("foo", node1).join();
    elector2.run("foo", node2).join();

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener(listener1).join();
    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener(listener2).join();
    LeaderEventListener listener3 = new LeaderEventListener();
    elector3.addListener(listener3).join();

    elector3.promote("foo", node3).thenAccept(result -> {
      assertFalse(result);
    }).join();

    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());
    assertFalse(listener3.hasEvent());

    elector3.run("foo", node3).join();

    listener1.nextEvent().thenAccept(result -> {
      assertEquals(node3, result.newLeadership().candidates().get(2));
    }).join();
    listener2.nextEvent().thenAccept(result -> {
      assertEquals(node3, result.newLeadership().candidates().get(2));
    }).join();
    listener3.nextEvent().thenAccept(result -> {
      assertEquals(node3, result.newLeadership().candidates().get(2));
    }).join();

    elector3.promote("foo", node3).thenAccept(result -> {
      assertTrue(result);
    }).join();

    listener1.nextEvent().thenAccept(result -> {
      assertEquals(node3, result.newLeadership().candidates().get(0));
    }).join();
    listener2.nextEvent().thenAccept(result -> {
      assertEquals(node3, result.newLeadership().candidates().get(0));
    }).join();
    listener3.nextEvent().thenAccept(result -> {
      assertEquals(node3, result.newLeadership().candidates().get(0));
    }).join();
  }

  @Test
  public void testLeaderSessionClose() throws Throwable {
    AsyncLeaderElector<NodeId> elector1 = atomix().<NodeId>leaderElectorBuilder("test-elector-leader-session-close").build().async();
    elector1.run("foo", node1).join();
    AsyncLeaderElector<NodeId> elector2 = atomix().<NodeId>leaderElectorBuilder("test-elector-leader-session-close").build().async();
    LeaderEventListener listener = new LeaderEventListener();
    elector2.run("foo", node2).join();
    elector2.addListener(listener).join();
    elector1.close();
    listener.nextEvent().thenAccept(result -> {
      assertEquals(node2, result.newLeadership().leader().id());
      assertEquals(1, result.newLeadership().candidates().size());
      assertEquals(node2, result.newLeadership().candidates().get(0));
    }).join();
  }

  @Test
  public void testNonLeaderSessionClose() throws Throwable {
    AsyncLeaderElector<NodeId> elector1 = atomix().<NodeId>leaderElectorBuilder("test-elector-non-leader-session-close").build().async();
    elector1.run("foo", node1).join();
    AsyncLeaderElector<NodeId> elector2 = atomix().<NodeId>leaderElectorBuilder("test-elector-non-leader-session-close").build().async();
    LeaderEventListener listener = new LeaderEventListener();
    elector2.run("foo", node2).join();
    elector1.addListener(listener).join();
    elector2.close().join();
    listener.nextEvent().thenAccept(result -> {
      assertEquals(node1, result.newLeadership().leader().id());
      assertEquals(1, result.newLeadership().candidates().size());
      assertEquals(node1, result.newLeadership().candidates().get(0));
    }).join();
  }

  @Test
  @Ignore // Leader balancing is currently not deterministic in this test
  public void testLeaderBalance() throws Throwable {
    AsyncLeaderElector<NodeId> elector1 = atomix().<NodeId>leaderElectorBuilder("test-elector-leader-balance").build().async();
    elector1.run("foo", node1).join();
    elector1.run("bar", node1).join();
    elector1.run("baz", node1).join();

    AsyncLeaderElector<NodeId> elector2 = atomix().<NodeId>leaderElectorBuilder("test-elector-leader-balance").build().async();
    elector2.run("foo", node2).join();
    elector2.run("bar", node2).join();
    elector2.run("baz", node2).join();

    AsyncLeaderElector<NodeId> elector3 = atomix().<NodeId>leaderElectorBuilder("test-elector-leader-balance").build().async();
    elector3.run("foo", node3).join();
    elector3.run("bar", node3).join();
    elector3.run("baz", node3).join();

    LeaderEventListener listener = new LeaderEventListener();
    elector2.addListener(listener).join();

    elector1.close();

    listener.nextEvent().thenAccept(result -> {
      assertEquals(node2, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().candidates().size());
      assertEquals(node2, result.newLeadership().candidates().get(0));
      assertEquals(node3, result.newLeadership().candidates().get(1));
    }).join();

    listener.nextEvent().thenAccept(result -> {
      assertEquals(node2, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().candidates().size());
      assertEquals(node2, result.newLeadership().candidates().get(0));
      assertEquals(node3, result.newLeadership().candidates().get(1));
    });

    listener.nextEvent().thenAccept(result -> {
      assertEquals(node3, result.newLeadership().leader().id());
      assertEquals(2, result.newLeadership().candidates().size());
      assertEquals(node3, result.newLeadership().candidates().get(0));
      assertEquals(node2, result.newLeadership().candidates().get(1));
    }).join();
  }

  @Test
  public void testQueries() throws Throwable {
    AsyncLeaderElector<NodeId> elector1 = atomix().<NodeId>leaderElectorBuilder("test-elector-query").build().async();
    AsyncLeaderElector<NodeId> elector2 = atomix().<NodeId>leaderElectorBuilder("test-elector-query").build().async();
    elector1.run("foo", node1).join();
    elector2.run("foo", node2).join();
    elector2.run("bar", node2).join();
    elector1.getLeadership("foo").thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(node1, result.candidates().get(0));
      assertEquals(node2, result.candidates().get(1));
    }).join();
    elector2.getLeadership("foo").thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(node1, result.candidates().get(0));
      assertEquals(node2, result.candidates().get(1));
    }).join();
    elector1.getLeadership("bar").thenAccept(result -> {
      assertEquals(node2, result.leader().id());
      assertEquals(node2, result.candidates().get(0));
    }).join();
    elector2.getLeadership("bar").thenAccept(result -> {
      assertEquals(node2, result.leader().id());
      assertEquals(node2, result.candidates().get(0));
    }).join();
    elector1.getLeaderships().thenAccept(result -> {
      assertEquals(2, result.size());
      Leadership fooLeadership = result.get("foo");
      assertEquals(node1, fooLeadership.leader().id());
      assertEquals(node1, fooLeadership.candidates().get(0));
      assertEquals(node2, fooLeadership.candidates().get(1));
      Leadership barLeadership = result.get("bar");
      assertEquals(node2, barLeadership.leader().id());
      assertEquals(node2, barLeadership.candidates().get(0));
    }).join();
    elector2.getLeaderships().thenAccept(result -> {
      assertEquals(2, result.size());
      Leadership fooLeadership = result.get("foo");
      assertEquals(node1, fooLeadership.leader().id());
      assertEquals(node1, fooLeadership.candidates().get(0));
      assertEquals(node2, fooLeadership.candidates().get(1));
      Leadership barLeadership = result.get("bar");
      assertEquals(node2, barLeadership.leader().id());
      assertEquals(node2, barLeadership.candidates().get(0));
    }).join();
  }

  private static class LeaderEventListener implements LeadershipEventListener<NodeId> {
    Queue<LeadershipEvent<NodeId>> eventQueue = new LinkedList<>();
    CompletableFuture<LeadershipEvent<NodeId>> pendingFuture;

    @Override
    public void onEvent(LeadershipEvent<NodeId> change) {
      synchronized (this) {
        if (pendingFuture != null) {
          pendingFuture.complete(change);
          pendingFuture = null;
        } else {
          eventQueue.add(change);
        }
      }
    }

    public boolean hasEvent() {
      return !eventQueue.isEmpty();
    }

    public void clearEvents() {
      eventQueue.clear();
    }

    public CompletableFuture<LeadershipEvent<NodeId>> nextEvent() {
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
