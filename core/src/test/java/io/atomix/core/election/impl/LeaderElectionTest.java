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
package io.atomix.core.election.impl;

import io.atomix.cluster.MemberId;
import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.election.AsyncLeaderElection;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.election.LeadershipEventListener;

import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link LeaderElectionProxy}.
 */
public abstract class LeaderElectionTest extends AbstractPrimitiveTest {

  MemberId node1 = MemberId.from("node1");
  MemberId node2 = MemberId.from("node2");
  MemberId node3 = MemberId.from("node3");

  @Test
  public void testRun() throws Throwable {
    AsyncLeaderElection<MemberId> election1 = atomix().<MemberId>leaderElectionBuilder("test-election-run", protocol()).build().async();
    election1.run(node1).thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(1, result.leader().term());
      assertEquals(1, result.candidates().size());
      assertEquals(node1, result.candidates().get(0));
    }).join();

    AsyncLeaderElection<MemberId> election2 = atomix().<MemberId>leaderElectionBuilder("test-election-run", protocol()).build().async();
    election2.run(node2).thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(1, result.leader().term());
      assertEquals(2, result.candidates().size());
      assertEquals(node1, result.candidates().get(0));
      assertEquals(node2, result.candidates().get(1));
    }).join();
  }

  @Test
  public void testWithdraw() throws Throwable {
    AsyncLeaderElection<MemberId> election1 = atomix().<MemberId>leaderElectionBuilder("test-election-withdraw", protocol()).build().async();
    election1.run(node1).join();
    AsyncLeaderElection<MemberId> election2 = atomix().<MemberId>leaderElectionBuilder("test-election-withdraw", protocol()).build().async();
    election2.run(node2).join();

    LeaderEventListener listener1 = new LeaderEventListener();
    election1.addListener(listener1).join();

    LeaderEventListener listener2 = new LeaderEventListener();
    election2.addListener(listener2).join();

    election1.withdraw(node1).join();

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

    Leadership<MemberId> leadership1 = election1.getLeadership().join();
    assertEquals(node2, leadership1.leader().id());
    assertEquals(1, leadership1.candidates().size());

    Leadership<MemberId> leadership2 = election2.getLeadership().join();
    assertEquals(node2, leadership2.leader().id());
    assertEquals(1, leadership2.candidates().size());
  }

  @Test
  public void testAnoint() throws Throwable {
    AsyncLeaderElection<MemberId> election1 = atomix().<MemberId>leaderElectionBuilder("test-election-anoint", protocol()).build().async();
    AsyncLeaderElection<MemberId> election2 = atomix().<MemberId>leaderElectionBuilder("test-election-anoint", protocol()).build().async();
    AsyncLeaderElection<MemberId> election3 = atomix().<MemberId>leaderElectionBuilder("test-election-anoint", protocol()).build().async();
    election1.run(node1).join();
    election2.run(node2).join();

    LeaderEventListener listener1 = new LeaderEventListener();
    election1.addListener(listener1).join();
    LeaderEventListener listener2 = new LeaderEventListener();
    election2.addListener(listener2);
    LeaderEventListener listener3 = new LeaderEventListener();
    election3.addListener(listener3).join();

    election3.anoint(node3).thenAccept(result -> {
      assertFalse(result);
    }).join();
    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());
    assertFalse(listener3.hasEvent());

    election3.anoint(node2).thenAccept(result -> {
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
    AsyncLeaderElection<MemberId> election1 = atomix().<MemberId>leaderElectionBuilder("test-election-promote", protocol()).build().async();
    AsyncLeaderElection<MemberId> election2 = atomix().<MemberId>leaderElectionBuilder("test-election-promote", protocol()).build().async();
    AsyncLeaderElection<MemberId> election3 = atomix().<MemberId>leaderElectionBuilder("test-election-promote", protocol()).build().async();
    election1.run(node1).join();
    election2.run(node2).join();

    LeaderEventListener listener1 = new LeaderEventListener();
    election1.addListener(listener1).join();
    LeaderEventListener listener2 = new LeaderEventListener();
    election2.addListener(listener2).join();
    LeaderEventListener listener3 = new LeaderEventListener();
    election3.addListener(listener3).join();

    election3.promote(node3).thenAccept(result -> {
      assertFalse(result);
    }).join();

    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());
    assertFalse(listener3.hasEvent());

    election3.run(node3).join();

    listener1.nextEvent().thenAccept(result -> {
      assertEquals(node3, result.newLeadership().candidates().get(2));
    }).join();
    listener2.nextEvent().thenAccept(result -> {
      assertEquals(node3, result.newLeadership().candidates().get(2));
    }).join();
    listener3.nextEvent().thenAccept(result -> {
      assertEquals(node3, result.newLeadership().candidates().get(2));
    }).join();

    election3.promote(node3).thenAccept(result -> {
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
    AsyncLeaderElection<MemberId> election1 = atomix().<MemberId>leaderElectionBuilder("test-election-leader-session-close", protocol()).build().async();
    election1.run(node1).join();
    AsyncLeaderElection<MemberId> election2 = atomix().<MemberId>leaderElectionBuilder("test-election-leader-session-close", protocol()).build().async();
    LeaderEventListener listener = new LeaderEventListener();
    election2.run(node2).join();
    election2.addListener(listener).join();
    election1.close();
    listener.nextEvent().thenAccept(result -> {
      assertEquals(node2, result.newLeadership().leader().id());
      assertEquals(1, result.newLeadership().candidates().size());
      assertEquals(node2, result.newLeadership().candidates().get(0));
    }).join();
  }

  @Test
  public void testNonLeaderSessionClose() throws Throwable {
    AsyncLeaderElection<MemberId> election1 = atomix().<MemberId>leaderElectionBuilder("test-election-non-leader-session-close", protocol()).build().async();
    election1.run(node1).join();
    AsyncLeaderElection<MemberId> election2 = atomix().<MemberId>leaderElectionBuilder("test-election-non-leader-session-close", protocol()).build().async();
    LeaderEventListener listener = new LeaderEventListener();
    election2.run(node2).join();
    election1.addListener(listener).join();
    election2.close().join();
    listener.nextEvent().thenAccept(result -> {
      assertEquals(node1, result.newLeadership().leader().id());
      assertEquals(1, result.newLeadership().candidates().size());
      assertEquals(node1, result.newLeadership().candidates().get(0));
    }).join();
  }

  @Test
  public void testQueries() throws Throwable {
    AsyncLeaderElection<MemberId> election1 = atomix().<MemberId>leaderElectionBuilder("test-election-query", protocol()).build().async();
    AsyncLeaderElection<MemberId> election2 = atomix().<MemberId>leaderElectionBuilder("test-election-query", protocol()).build().async();
    election1.run(node1).join();
    election2.run(node2).join();
    election2.run(node2).join();
    election1.getLeadership().thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(node1, result.candidates().get(0));
      assertEquals(node2, result.candidates().get(1));
    }).join();
    election2.getLeadership().thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(node1, result.candidates().get(0));
      assertEquals(node2, result.candidates().get(1));
    }).join();
  }

  private static class LeaderEventListener implements LeadershipEventListener<MemberId> {
    Queue<LeadershipEvent<MemberId>> eventQueue = new LinkedList<>();
    CompletableFuture<LeadershipEvent<MemberId>> pendingFuture;

    @Override
    public void onEvent(LeadershipEvent<MemberId> event) {
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

    public CompletableFuture<LeadershipEvent<MemberId>> nextEvent() {
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
