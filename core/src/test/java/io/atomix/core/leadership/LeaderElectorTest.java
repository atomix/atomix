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
package io.atomix.core.leadership;

import io.atomix.cluster.MemberId;
import io.atomix.core.AbstractPrimitiveTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Leader elector test.
 */
public abstract class LeaderElectorTest extends AbstractPrimitiveTest {

  MemberId node1 = MemberId.from("4");
  MemberId node2 = MemberId.from("5");
  MemberId node3 = MemberId.from("6");

  @Test
  public void testRun() throws Throwable {
    AsyncLeaderElector<MemberId> elector1 = atomix().<MemberId>leaderElectorBuilder("test-elector-run", protocol()).build().async();
    elector1.run("foo", node1).thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(1, result.leader().term());
      assertEquals(1, result.candidates().size());
      assertEquals(node1, result.candidates().get(0));
    }).get(30, TimeUnit.SECONDS);

    elector1.run("bar", node1).thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(1, result.leader().term());
      assertEquals(1, result.candidates().size());
      assertEquals(node1, result.candidates().get(0));
    }).get(30, TimeUnit.SECONDS);

    AsyncLeaderElector<MemberId> elector2 = atomix().<MemberId>leaderElectorBuilder("test-elector-run", protocol()).build().async();
    elector2.run("bar", node2).thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(1, result.leader().term());
      assertEquals(2, result.candidates().size());
      assertEquals(node1, result.candidates().get(0));
      assertEquals(node2, result.candidates().get(1));
    }).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testWithdraw() throws Throwable {
    AsyncLeaderElector<MemberId> elector1 = atomix().<MemberId>leaderElectorBuilder("test-elector-withdraw", protocol()).build().async();
    elector1.run("foo", node1).get(30, TimeUnit.SECONDS);
    AsyncLeaderElector<MemberId> elector2 = atomix().<MemberId>leaderElectorBuilder("test-elector-withdraw", protocol()).build().async();
    elector2.run("foo", node2).get(30, TimeUnit.SECONDS);

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener("foo", listener1).get(30, TimeUnit.SECONDS);

    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener("foo", listener2).get(30, TimeUnit.SECONDS);

    LeaderEventListener listener3 = new LeaderEventListener();
    elector1.addListener("bar", listener3);
    elector2.addListener("bar", listener3);

    elector1.withdraw("foo", node1).get(30, TimeUnit.SECONDS);

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

    assertFalse(listener3.hasEvent());

    Leadership leadership1 = elector1.getLeadership("foo").get(30, TimeUnit.SECONDS);
    assertEquals(node2, leadership1.leader().id());
    assertEquals(1, leadership1.candidates().size());

    Leadership leadership2 = elector2.getLeadership("foo").get(30, TimeUnit.SECONDS);
    assertEquals(node2, leadership2.leader().id());
    assertEquals(1, leadership2.candidates().size());
  }

  @Test
  public void testAnoint() throws Throwable {
    AsyncLeaderElector<MemberId> elector1 = atomix().<MemberId>leaderElectorBuilder("test-elector-anoint", protocol()).build().async();
    AsyncLeaderElector<MemberId> elector2 = atomix().<MemberId>leaderElectorBuilder("test-elector-anoint", protocol()).build().async();
    AsyncLeaderElector<MemberId> elector3 = atomix().<MemberId>leaderElectorBuilder("test-elector-anoint", protocol()).build().async();
    elector1.run("foo", node1).get(30, TimeUnit.SECONDS);
    elector2.run("foo", node2).get(30, TimeUnit.SECONDS);

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener("foo", listener1).get(30, TimeUnit.SECONDS);
    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener(listener2);
    LeaderEventListener listener3 = new LeaderEventListener();
    elector3.addListener("foo", listener3).get(30, TimeUnit.SECONDS);

    elector3.anoint("foo", node3).thenAccept(result -> {
      assertFalse(result);
    }).get(30, TimeUnit.SECONDS);
    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());
    assertFalse(listener3.hasEvent());

    elector3.anoint("foo", node2).thenAccept(result -> {
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
    AsyncLeaderElector<MemberId> elector1 = atomix().<MemberId>leaderElectorBuilder("test-elector-promote", protocol()).build().async();
    AsyncLeaderElector<MemberId> elector2 = atomix().<MemberId>leaderElectorBuilder("test-elector-promote", protocol()).build().async();
    AsyncLeaderElector<MemberId> elector3 = atomix().<MemberId>leaderElectorBuilder("test-elector-promote", protocol()).build().async();
    elector1.run("foo", node1).get(30, TimeUnit.SECONDS);
    elector2.run("foo", node2).get(30, TimeUnit.SECONDS);

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener("foo", listener1).get(30, TimeUnit.SECONDS);
    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener(listener2).get(30, TimeUnit.SECONDS);
    LeaderEventListener listener3 = new LeaderEventListener();
    elector3.addListener(listener3).get(30, TimeUnit.SECONDS);

    elector3.promote("foo", node3).thenAccept(result -> {
      assertFalse(result);
    }).get(30, TimeUnit.SECONDS);

    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());
    assertFalse(listener3.hasEvent());

    elector3.run("foo", node3).get(30, TimeUnit.SECONDS);

    listener1.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(2));
    }).get(30, TimeUnit.SECONDS);
    listener2.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(2));
    }).get(30, TimeUnit.SECONDS);
    listener3.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(2));
    }).get(30, TimeUnit.SECONDS);

    elector3.promote("foo", node3).thenAccept(result -> {
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
    AsyncLeaderElector<MemberId> elector1 = atomix().<MemberId>leaderElectorBuilder("test-elector-leader-session-close", protocol()).build().async();
    elector1.run("foo", node1).get(30, TimeUnit.SECONDS);
    AsyncLeaderElector<MemberId> elector2 = atomix().<MemberId>leaderElectorBuilder("test-elector-leader-session-close", protocol()).build().async();
    LeaderEventListener listener = new LeaderEventListener();
    elector2.run("foo", node2).get(30, TimeUnit.SECONDS);
    elector2.addListener("foo", listener).get(30, TimeUnit.SECONDS);
    elector1.close();
    listener.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(1, result.newLeadership().candidates().size());
      Assert.assertEquals(node2, result.newLeadership().candidates().get(0));
    }).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testNonLeaderSessionClose() throws Throwable {
    AsyncLeaderElector<MemberId> elector1 = atomix().<MemberId>leaderElectorBuilder("test-elector-non-leader-session-close", protocol()).build().async();
    elector1.run("foo", node1).get(30, TimeUnit.SECONDS);
    AsyncLeaderElector<MemberId> elector2 = atomix().<MemberId>leaderElectorBuilder("test-elector-non-leader-session-close", protocol()).build().async();
    LeaderEventListener listener = new LeaderEventListener();
    elector2.run("foo", node2).get(30, TimeUnit.SECONDS);
    elector1.addListener(listener).get(30, TimeUnit.SECONDS);
    elector2.close().get(30, TimeUnit.SECONDS);
    listener.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node1, result.newLeadership().leader().id());
      Assert.assertEquals(1, result.newLeadership().candidates().size());
      Assert.assertEquals(node1, result.newLeadership().candidates().get(0));
    }).get(30, TimeUnit.SECONDS);
  }

  @Test
  @Ignore // Leader balancing is currently not deterministic in this test
  public void testLeaderBalance() throws Throwable {
    AsyncLeaderElector<MemberId> elector1 = atomix().<MemberId>leaderElectorBuilder("test-elector-leader-balance", protocol()).build().async();
    elector1.run("foo", node1).get(30, TimeUnit.SECONDS);
    elector1.run("bar", node1).get(30, TimeUnit.SECONDS);
    elector1.run("baz", node1).get(30, TimeUnit.SECONDS);

    AsyncLeaderElector<MemberId> elector2 = atomix().<MemberId>leaderElectorBuilder("test-elector-leader-balance", protocol()).build().async();
    elector2.run("foo", node2).get(30, TimeUnit.SECONDS);
    elector2.run("bar", node2).get(30, TimeUnit.SECONDS);
    elector2.run("baz", node2).get(30, TimeUnit.SECONDS);

    AsyncLeaderElector<MemberId> elector3 = atomix().<MemberId>leaderElectorBuilder("test-elector-leader-balance", protocol()).build().async();
    elector3.run("foo", node3).get(30, TimeUnit.SECONDS);
    elector3.run("bar", node3).get(30, TimeUnit.SECONDS);
    elector3.run("baz", node3).get(30, TimeUnit.SECONDS);

    LeaderEventListener listener = new LeaderEventListener();
    elector2.addListener("foo", listener).get(30, TimeUnit.SECONDS);

    elector1.close();

    listener.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().candidates().size());
      Assert.assertEquals(node2, result.newLeadership().candidates().get(0));
      Assert.assertEquals(node3, result.newLeadership().candidates().get(1));
    }).get(30, TimeUnit.SECONDS);

    listener.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().candidates().size());
      Assert.assertEquals(node2, result.newLeadership().candidates().get(0));
      Assert.assertEquals(node3, result.newLeadership().candidates().get(1));
    });

    listener.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().candidates().size());
      Assert.assertEquals(node3, result.newLeadership().candidates().get(0));
      Assert.assertEquals(node2, result.newLeadership().candidates().get(1));
    }).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testQueries() throws Throwable {
    AsyncLeaderElector<MemberId> elector1 = atomix().<MemberId>leaderElectorBuilder("test-elector-query", protocol()).build().async();
    AsyncLeaderElector<MemberId> elector2 = atomix().<MemberId>leaderElectorBuilder("test-elector-query", protocol()).build().async();
    elector1.run("foo", node1).get(30, TimeUnit.SECONDS);
    elector2.run("foo", node2).get(30, TimeUnit.SECONDS);
    elector2.run("bar", node2).get(30, TimeUnit.SECONDS);
    elector1.getLeadership("foo").thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(node1, result.candidates().get(0));
      assertEquals(node2, result.candidates().get(1));
    }).get(30, TimeUnit.SECONDS);
    elector2.getLeadership("foo").thenAccept(result -> {
      assertEquals(node1, result.leader().id());
      assertEquals(node1, result.candidates().get(0));
      assertEquals(node2, result.candidates().get(1));
    }).get(30, TimeUnit.SECONDS);
    elector1.getLeadership("bar").thenAccept(result -> {
      assertEquals(node2, result.leader().id());
      assertEquals(node2, result.candidates().get(0));
    }).get(30, TimeUnit.SECONDS);
    elector2.getLeadership("bar").thenAccept(result -> {
      assertEquals(node2, result.leader().id());
      assertEquals(node2, result.candidates().get(0));
    }).get(30, TimeUnit.SECONDS);
    elector1.getLeaderships().thenAccept(result -> {
      assertEquals(2, result.size());
      Leadership fooLeadership = result.get("foo");
      assertEquals(node1, fooLeadership.leader().id());
      assertEquals(node1, fooLeadership.candidates().get(0));
      assertEquals(node2, fooLeadership.candidates().get(1));
      Leadership barLeadership = result.get("bar");
      assertEquals(node2, barLeadership.leader().id());
      assertEquals(node2, barLeadership.candidates().get(0));
    }).get(30, TimeUnit.SECONDS);
    elector2.getLeaderships().thenAccept(result -> {
      assertEquals(2, result.size());
      Leadership fooLeadership = result.get("foo");
      assertEquals(node1, fooLeadership.leader().id());
      assertEquals(node1, fooLeadership.candidates().get(0));
      assertEquals(node2, fooLeadership.candidates().get(1));
      Leadership barLeadership = result.get("bar");
      assertEquals(node2, barLeadership.leader().id());
      assertEquals(node2, barLeadership.candidates().get(0));
    }).get(30, TimeUnit.SECONDS);
  }

  private static class LeaderEventListener implements LeadershipEventListener<MemberId> {
    Queue<LeadershipEvent<MemberId>> eventQueue = new LinkedList<>();
    CompletableFuture<LeadershipEvent<MemberId>> pendingFuture;

    @Override
    public void event(LeadershipEvent<MemberId> change) {
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
