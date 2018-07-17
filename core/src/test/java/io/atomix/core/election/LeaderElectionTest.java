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
package io.atomix.core.election;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.election.impl.LeaderElectionProxy;
import io.atomix.primitive.protocol.ProxyProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link LeaderElectionProxy}.
 */
public abstract class LeaderElectionTest extends AbstractPrimitiveTest<ProxyProtocol> {
  String node1 = "node1";
  String node2 = "node2";
  String node3 = "node3";

  @Test
  public void testRun() throws Throwable {
    AsyncLeaderElection<String> election1 = atomix().<String>leaderElectionBuilder("test-election-run")
        .withProtocol(protocol())
        .build()
        .async();
    election1.run(node1).thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(1, result.leader().term());
      assertEquals(1, result.candidates().size());
      assertEquals(node1, result.candidates().get(0));
    }).get(30, TimeUnit.SECONDS);

    AsyncLeaderElection<String> election2 = atomix().<String>leaderElectionBuilder("test-election-run")
        .withProtocol(protocol())
        .build()
        .async();
    election2.run(node2).thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(1, result.leader().term());
      assertEquals(2, result.candidates().size());
      assertEquals(node1, result.candidates().get(0));
      assertEquals(node2, result.candidates().get(1));
    }).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testWithdraw() throws Throwable {
    AsyncLeaderElection<String> election1 = atomix().<String>leaderElectionBuilder("test-election-withdraw")
        .withProtocol(protocol())
        .build()
        .async();
    election1.run(node1).get(30, TimeUnit.SECONDS);
    AsyncLeaderElection<String> election2 = atomix().<String>leaderElectionBuilder("test-election-withdraw")
        .withProtocol(protocol())
        .build()
        .async();
    election2.run(node2).get(30, TimeUnit.SECONDS);

    LeaderEventListener listener1 = new LeaderEventListener();
    election1.addListener(listener1).get(30, TimeUnit.SECONDS);

    LeaderEventListener listener2 = new LeaderEventListener();
    election2.addListener(listener2).get(30, TimeUnit.SECONDS);

    election1.withdraw(node1).get(30, TimeUnit.SECONDS);

    listener1.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().leader().term());
      Assert.assertEquals(1, result.newLeadership().candidates().size());
      Assert.assertEquals(node2, result.newLeadership().candidates().get(0));
    }).get(30, TimeUnit.SECONDS);

    listener2.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().leader().term());
      Assert.assertEquals(1, result.newLeadership().candidates().size());
      Assert.assertEquals(node2, result.newLeadership().candidates().get(0));
    }).get(30, TimeUnit.SECONDS);

    Leadership<String> leadership1 = election1.getLeadership().get(30, TimeUnit.SECONDS);
    assertEquals(node2, leadership1.leader().id());
    assertEquals(1, leadership1.candidates().size());

    Leadership<String> leadership2 = election2.getLeadership().get(30, TimeUnit.SECONDS);
    assertEquals(node2, leadership2.leader().id());
    assertEquals(1, leadership2.candidates().size());
  }

  @Test
  public void testAnoint() throws Throwable {
    AsyncLeaderElection<String> election1 = atomix().<String>leaderElectionBuilder("test-election-anoint")
        .withProtocol(protocol())
        .build()
        .async();
    AsyncLeaderElection<String> election2 = atomix().<String>leaderElectionBuilder("test-election-anoint")
        .withProtocol(protocol())
        .build()
        .async();
    AsyncLeaderElection<String> election3 = atomix().<String>leaderElectionBuilder("test-election-anoint")
        .withProtocol(protocol())
        .build()
        .async();
    election1.run(node1).get(30, TimeUnit.SECONDS);
    election2.run(node2).get(30, TimeUnit.SECONDS);

    LeaderEventListener listener1 = new LeaderEventListener();
    election1.addListener(listener1).get(30, TimeUnit.SECONDS);
    LeaderEventListener listener2 = new LeaderEventListener();
    election2.addListener(listener2);
    LeaderEventListener listener3 = new LeaderEventListener();
    election3.addListener(listener3).get(30, TimeUnit.SECONDS);

    election3.anoint(node3).thenAccept(result -> {
      assertFalse(result);
    }).get(30, TimeUnit.SECONDS);
    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());
    assertFalse(listener3.hasEvent());

    election3.anoint(node2).thenAccept(result -> {
      assertTrue(result);
    }).get(30, TimeUnit.SECONDS);

    listener1.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().candidates().size());
      Assert.assertEquals(node1, result.newLeadership().candidates().get(0));
      Assert.assertEquals(node2, result.newLeadership().candidates().get(1));
    }).get(30, TimeUnit.SECONDS);
    listener2.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().candidates().size());
      Assert.assertEquals(node1, result.newLeadership().candidates().get(0));
      Assert.assertEquals(node2, result.newLeadership().candidates().get(1));
    }).get(30, TimeUnit.SECONDS);
    listener3.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().candidates().size());
      Assert.assertEquals(node1, result.newLeadership().candidates().get(0));
      Assert.assertEquals(node2, result.newLeadership().candidates().get(1));
    }).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testPromote() throws Throwable {
    AsyncLeaderElection<String> election1 = atomix().<String>leaderElectionBuilder("test-election-promote")
        .withProtocol(protocol())
        .build()
        .async();
    AsyncLeaderElection<String> election2 = atomix().<String>leaderElectionBuilder("test-election-promote")
        .withProtocol(protocol())
        .build()
        .async();
    AsyncLeaderElection<String> election3 = atomix().<String>leaderElectionBuilder("test-election-promote")
        .withProtocol(protocol())
        .build()
        .async();
    election1.run(node1).get(30, TimeUnit.SECONDS);
    election2.run(node2).get(30, TimeUnit.SECONDS);

    LeaderEventListener listener1 = new LeaderEventListener();
    election1.addListener(listener1).get(30, TimeUnit.SECONDS);
    LeaderEventListener listener2 = new LeaderEventListener();
    election2.addListener(listener2).get(30, TimeUnit.SECONDS);
    LeaderEventListener listener3 = new LeaderEventListener();
    election3.addListener(listener3).get(30, TimeUnit.SECONDS);

    election3.promote(node3).thenAccept(result -> {
      assertFalse(result);
    }).get(30, TimeUnit.SECONDS);

    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());
    assertFalse(listener3.hasEvent());

    election3.run(node3).get(30, TimeUnit.SECONDS);

    listener1.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(2));
    }).get(30, TimeUnit.SECONDS);
    listener2.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(2));
    }).get(30, TimeUnit.SECONDS);
    listener3.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(2));
    }).get(30, TimeUnit.SECONDS);

    election3.promote(node3).thenAccept(result -> {
      assertTrue(result);
    }).get(30, TimeUnit.SECONDS);

    listener1.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(0));
    }).get(30, TimeUnit.SECONDS);
    listener2.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(0));
    }).get(30, TimeUnit.SECONDS);
    listener3.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(0));
    }).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testLeaderSessionClose() throws Throwable {
    AsyncLeaderElection<String> election1 = atomix().<String>leaderElectionBuilder("test-election-leader-session-close")
        .withProtocol(protocol())
        .build()
        .async();
    election1.run(node1).get(30, TimeUnit.SECONDS);
    AsyncLeaderElection<String> election2 = atomix().<String>leaderElectionBuilder("test-election-leader-session-close")
        .withProtocol(protocol())
        .build()
        .async();
    LeaderEventListener listener = new LeaderEventListener();
    election2.run(node2).get(30, TimeUnit.SECONDS);
    election2.addListener(listener).get(30, TimeUnit.SECONDS);
    election1.close();
    listener.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(1, result.newLeadership().candidates().size());
      Assert.assertEquals(node2, result.newLeadership().candidates().get(0));
    }).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testNonLeaderSessionClose() throws Throwable {
    AsyncLeaderElection<String> election1 = atomix().<String>leaderElectionBuilder("test-election-non-leader-session-close")
        .withProtocol(protocol())
        .build()
        .async();
    election1.run(node1).get(30, TimeUnit.SECONDS);
    AsyncLeaderElection<String> election2 = atomix().<String>leaderElectionBuilder("test-election-non-leader-session-close")
        .withProtocol(protocol())
        .build()
        .async();
    LeaderEventListener listener = new LeaderEventListener();
    election2.run(node2).get(30, TimeUnit.SECONDS);
    election1.addListener(listener).get(30, TimeUnit.SECONDS);
    election2.close().get(30, TimeUnit.SECONDS);
    listener.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node1, result.newLeadership().leader().id());
      Assert.assertEquals(1, result.newLeadership().candidates().size());
      Assert.assertEquals(node1, result.newLeadership().candidates().get(0));
    }).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testQueries() throws Throwable {
    AsyncLeaderElection<String> election1 = atomix().<String>leaderElectionBuilder("test-election-query")
        .withProtocol(protocol())
        .build()
        .async();
    AsyncLeaderElection<String> election2 = atomix().<String>leaderElectionBuilder("test-election-query")
        .withProtocol(protocol())
        .build()
        .async();
    election1.run(node1).get(30, TimeUnit.SECONDS);
    election2.run(node2).get(30, TimeUnit.SECONDS);
    election2.run(node2).get(30, TimeUnit.SECONDS);
    election1.getLeadership().thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(node1, result.candidates().get(0));
      assertEquals(node2, result.candidates().get(1));
    }).get(30, TimeUnit.SECONDS);
    election2.getLeadership().thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(node1, result.candidates().get(0));
      assertEquals(node2, result.candidates().get(1));
    }).get(30, TimeUnit.SECONDS);
  }

  private static class LeaderEventListener implements LeadershipEventListener<String> {
    Queue<LeadershipEvent<String>> eventQueue = new LinkedList<>();
    CompletableFuture<LeadershipEvent<String>> pendingFuture;

    @Override
    public void event(LeadershipEvent<String> event) {
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

    public CompletableFuture<LeadershipEvent<String>> nextEvent() {
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
