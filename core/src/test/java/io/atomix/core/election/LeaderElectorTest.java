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
package io.atomix.core.election;

import io.atomix.core.AbstractPrimitiveTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Leader elector test.
 */
public class LeaderElectorTest extends AbstractPrimitiveTest {
  String node1 = "4";
  String node2 = "5";
  String node3 = "6";

  @Test
  public void testRun() throws Throwable {
    LeaderElector<String> elector1 = atomix().<String>leaderElectorBuilder("test-elector-run")
        .withProtocol(protocol())
        .build();
    Leadership<String> fooLeadership = elector1.run("foo", node1);
    assertEquals(node1, fooLeadership.leader().id());
    assertEquals(1, fooLeadership.leader().term());
    assertEquals(1, fooLeadership.candidates().size());
    assertEquals(node1, fooLeadership.candidates().get(0));

    Leadership<String> barLeadership = elector1.run("bar", node1);
    assertEquals(node1, barLeadership.leader().id());
    assertEquals(1, barLeadership.leader().term());
    assertEquals(1, barLeadership.candidates().size());
    assertEquals(node1, barLeadership.candidates().get(0));

    LeaderElector<String> elector2 = atomix().<String>leaderElectorBuilder("test-elector-run")
        .withProtocol(protocol())
        .build();
    Leadership<String> barLeadership2 = elector2.run("bar", node2);
    assertEquals(node1, barLeadership2.leader().id());
    assertEquals(1, barLeadership2.leader().term());
    assertEquals(2, barLeadership2.candidates().size());
    assertEquals(node1, barLeadership2.candidates().get(0));
    assertEquals(node2, barLeadership2.candidates().get(1));
  }

  @Test
  public void testWithdraw() throws Throwable {
    LeaderElector<String> elector1 = atomix().<String>leaderElectorBuilder("test-elector-withdraw")
        .withProtocol(protocol())
        .build();
    elector1.run("foo", node1);
    LeaderElector<String> elector2 = atomix().<String>leaderElectorBuilder("test-elector-withdraw")
        .withProtocol(protocol())
        .build();
    elector2.run("foo", node2);

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener("foo", listener1);

    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener("foo", listener2);

    LeaderEventListener listener3 = new LeaderEventListener();
    elector1.addListener("bar", listener3);
    elector2.addListener("bar", listener3);

    elector1.withdraw("foo", node1);

    listener1.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().leader().term());
      Assert.assertEquals(1, result.newLeadership().candidates().size());
      Assert.assertEquals(node2, result.newLeadership().candidates().get(0));
    });

    listener2.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().leader().term());
      Assert.assertEquals(1, result.newLeadership().candidates().size());
      Assert.assertEquals(node2, result.newLeadership().candidates().get(0));
    });

    assertFalse(listener3.hasEvent());

    Leadership leadership1 = elector1.getLeadership("foo");
    assertEquals(node2, leadership1.leader().id());
    assertEquals(1, leadership1.candidates().size());

    Leadership leadership2 = elector2.getLeadership("foo");
    assertEquals(node2, leadership2.leader().id());
    assertEquals(1, leadership2.candidates().size());
  }

  @Test
  public void testAnoint() throws Throwable {
    LeaderElector<String> elector1 = atomix().<String>leaderElectorBuilder("test-elector-anoint")
        .withProtocol(protocol())
        .build();
    LeaderElector<String> elector2 = atomix().<String>leaderElectorBuilder("test-elector-anoint")
        .withProtocol(protocol())
        .build();
    LeaderElector<String> elector3 = atomix().<String>leaderElectorBuilder("test-elector-anoint")
        .withProtocol(protocol())
        .build();
    elector1.run("foo", node1);
    elector2.run("foo", node2);

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener("foo", listener1);
    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener(listener2);
    LeaderEventListener listener3 = new LeaderEventListener();
    elector3.addListener("foo", listener3);

    assertFalse(elector3.anoint("foo", node3));
    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());
    assertFalse(listener3.hasEvent());

    assertTrue(elector3.anoint("foo", node2));

    listener1.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().candidates().size());
      Assert.assertEquals(node1, result.newLeadership().candidates().get(0));
      Assert.assertEquals(node2, result.newLeadership().candidates().get(1));
    });
    listener2.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().candidates().size());
      Assert.assertEquals(node1, result.newLeadership().candidates().get(0));
      Assert.assertEquals(node2, result.newLeadership().candidates().get(1));
    });
    listener3.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().candidates().size());
      Assert.assertEquals(node1, result.newLeadership().candidates().get(0));
      Assert.assertEquals(node2, result.newLeadership().candidates().get(1));
    });
  }

  @Test
  public void testPromote() throws Throwable {
    LeaderElector<String> elector1 = atomix().<String>leaderElectorBuilder("test-elector-promote")
        .withProtocol(protocol())
        .build();
    LeaderElector<String> elector2 = atomix().<String>leaderElectorBuilder("test-elector-promote")
        .withProtocol(protocol())
        .build();
    LeaderElector<String> elector3 = atomix().<String>leaderElectorBuilder("test-elector-promote")
        .withProtocol(protocol())
        .build();
    elector1.run("foo", node1);
    elector2.run("foo", node2);

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener("foo", listener1);
    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener(listener2);
    LeaderEventListener listener3 = new LeaderEventListener();
    elector3.addListener(listener3);

    assertFalse(elector3.promote("foo", node3));

    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());
    assertFalse(listener3.hasEvent());

    elector3.run("foo", node3);

    listener1.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(2));
    });
    listener2.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(2));
    });
    listener3.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(2));
    });

    assertTrue(elector3.promote("foo", node3));

    listener1.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(0));
    });
    listener2.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(0));
    });
    listener3.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node3, result.newLeadership().candidates().get(0));
    });
  }

  @Test
  public void testLeaderSessionClose() throws Throwable {
    LeaderElector<String> elector1 = atomix().<String>leaderElectorBuilder("test-elector-leader-session-close")
        .withProtocol(protocol())
        .build();
    elector1.run("foo", node1);
    LeaderElector<String> elector2 = atomix().<String>leaderElectorBuilder("test-elector-leader-session-close")
        .withProtocol(protocol())
        .build();
    LeaderEventListener listener = new LeaderEventListener();
    elector2.run("foo", node2);
    elector2.addListener("foo", listener);
    elector1.close();
    listener.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(1, result.newLeadership().candidates().size());
      Assert.assertEquals(node2, result.newLeadership().candidates().get(0));
    });
  }

  @Test
  public void testNonLeaderSessionClose() throws Throwable {
    LeaderElector<String> elector1 = atomix().<String>leaderElectorBuilder("test-elector-non-leader-session-close")
        .withProtocol(protocol())
        .build();
    elector1.run("foo", node1);
    LeaderElector<String> elector2 = atomix().<String>leaderElectorBuilder("test-elector-non-leader-session-close")
        .withProtocol(protocol())
        .build();
    LeaderEventListener listener = new LeaderEventListener();
    elector2.run("foo", node2);
    elector1.addListener(listener);
    elector2.close();
    listener.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node1, result.newLeadership().leader().id());
      Assert.assertEquals(1, result.newLeadership().candidates().size());
      Assert.assertEquals(node1, result.newLeadership().candidates().get(0));
    });
  }

  @Test
  @Ignore // Leader balancing is currently not deterministic in this test
  public void testLeaderBalance() throws Throwable {
    LeaderElector<String> elector1 = atomix().<String>leaderElectorBuilder("test-elector-leader-balance")
        .withProtocol(protocol())
        .build();
    elector1.run("foo", node1);
    elector1.run("bar", node1);
    elector1.run("baz", node1);

    LeaderElector<String> elector2 = atomix().<String>leaderElectorBuilder("test-elector-leader-balance")
        .withProtocol(protocol())
        .build();
    elector2.run("foo", node2);
    elector2.run("bar", node2);
    elector2.run("baz", node2);

    LeaderElector<String> elector3 = atomix().<String>leaderElectorBuilder("test-elector-leader-balance")
        .withProtocol(protocol())
        .build();
    elector3.run("foo", node3);
    elector3.run("bar", node3);
    elector3.run("baz", node3);

    LeaderEventListener listener = new LeaderEventListener();
    elector2.addListener("foo", listener);

    elector1.close();

    listener.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node2, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().candidates().size());
      Assert.assertEquals(node2, result.newLeadership().candidates().get(0));
      Assert.assertEquals(node3, result.newLeadership().candidates().get(1));
    });

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
    });
  }

  @Test
  public void testQueries() throws Throwable {
    LeaderElector<String> elector1 = atomix().<String>leaderElectorBuilder("test-elector-query")
        .withProtocol(protocol())
        .build();
    LeaderElector<String> elector2 = atomix().<String>leaderElectorBuilder("test-elector-query")
        .withProtocol(protocol())
        .build();
    elector1.run("foo", node1);
    elector2.run("foo", node2);
    elector2.run("bar", node2);

    Leadership<String> leadership1 = elector1.getLeadership("foo");
    assertEquals(node1, leadership1.leader().id());
    assertEquals(node1, leadership1.candidates().get(0));
    assertEquals(node2, leadership1.candidates().get(1));

    Leadership<String> leadership2 = elector2.getLeadership("foo");
    assertEquals(node1, leadership2.leader().id());
    assertEquals(node1, leadership2.candidates().get(0));
    assertEquals(node2, leadership2.candidates().get(1));

    leadership1 = elector1.getLeadership("bar");
    assertEquals(node2, leadership1.leader().id());
    assertEquals(node2, leadership1.candidates().get(0));

    leadership2 = elector2.getLeadership("bar");
    assertEquals(node2, leadership2.leader().id());
    assertEquals(node2, leadership2.candidates().get(0));

    Map<String, Leadership<String>> leaderships1 = elector1.getLeaderships();
    assertEquals(2, leaderships1.size());
    Leadership fooLeadership1 = leaderships1.get("foo");
    assertEquals(node1, fooLeadership1.leader().id());
    assertEquals(node1, fooLeadership1.candidates().get(0));
    assertEquals(node2, fooLeadership1.candidates().get(1));
    Leadership barLeadership1 = leaderships1.get("bar");
    assertEquals(node2, barLeadership1.leader().id());
    assertEquals(node2, barLeadership1.candidates().get(0));

    Map<String, Leadership<String>> leaderships2 = elector2.getLeaderships();
    assertEquals(2, leaderships2.size());
    Leadership fooLeadership2 = leaderships2.get("foo");
    assertEquals(node1, fooLeadership2.leader().id());
    assertEquals(node1, fooLeadership2.candidates().get(0));
    assertEquals(node2, fooLeadership2.candidates().get(1));
    Leadership barLeadership2 = leaderships2.get("bar");
    assertEquals(node2, barLeadership2.leader().id());
    assertEquals(node2, barLeadership2.candidates().get(0));
  }

  @Test
  public void testCache() throws Throwable {
    LeaderElector<String> elector1 = atomix().<String>leaderElectorBuilder("test-cache")
        .withProtocol(protocol())
        .withCacheEnabled()
        .build();
    LeaderElector<String> elector2 = atomix().<String>leaderElectorBuilder("test-cache")
        .withProtocol(protocol())
        .withCacheEnabled()
        .build();

    elector1.run("foo", node1);

    LeaderEventListener listener1 = new LeaderEventListener();
    elector1.addListener("foo", listener1);
    LeaderEventListener listener2 = new LeaderEventListener();
    elector2.addListener(listener2);

    assertFalse(listener1.hasEvent());
    assertFalse(listener2.hasEvent());

    elector2.run("foo", node2);

    listener1.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node1, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().candidates().size());
      Assert.assertEquals(node1, result.newLeadership().candidates().get(0));
      Assert.assertEquals(node2, result.newLeadership().candidates().get(1));
    }).join();
    listener2.nextEvent().thenAccept(result -> {
      Assert.assertEquals(node1, result.newLeadership().leader().id());
      Assert.assertEquals(2, result.newLeadership().candidates().size());
      Assert.assertEquals(node1, result.newLeadership().candidates().get(0));
      Assert.assertEquals(node2, result.newLeadership().candidates().get(1));
    }).join();

    elector1.withdraw("foo", node1);
    listener2.nextEvent().thenAccept(result -> {
      assertEquals(node2, result.newLeadership().leader().id());
    }).join();
    assertEquals(node2, elector1.getLeadership("foo").leader().id());
    assertEquals(node2, elector2.getLeadership("foo").leader().id());
  }

  private static class LeaderEventListener implements LeadershipEventListener<String> {
    Queue<LeadershipEvent<String>> eventQueue = new LinkedList<>();
    CompletableFuture<LeadershipEvent<String>> pendingFuture;

    @Override
    public void event(LeadershipEvent<String> change) {
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
