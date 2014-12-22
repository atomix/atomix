/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.internal;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogConfig;
import net.kuujo.copycat.spi.ExecutionContext;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

/**
 * Copycat state context tests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class CopycatStateContextTest extends ConcurrentTestCase {

  /**
   * Creates a new state context.
   */
  private CopycatStateContext createContext() {
    ClusterConfig cluster = new ClusterConfig()
      .withMembers("local://foo", "local://bar", "local://baz");
    Log log = new BufferedLog("test", new LogConfig());
    return new CopycatStateContext("local://foo", cluster, log, ExecutionContext.create());
  }

  /**
   * Tests that the voted for status is reset when a new leader is found.
   */
  public void testLeaderChangeResetsVotedFor() {
    CopycatStateContext context = createContext();
    context.setLastVotedFor("local://bar");
    assertEquals("local://bar", context.getLastVotedFor());
    context.setLeader("local://bar");
    assertNull(context.getLastVotedFor());
  }

  /**
   * Tests that a node times out and starts a new election on startup.
   */
  public void testInitialStateEventuallyTransitionsToFollowerAndSendsPollRequests() throws Throwable {
    CountDownLatch latch = new CountDownLatch(1);
    CopycatStateContext context = createContext();
    context.open().get();
    context.pollHandler(request -> {
      threadAssertTrue(true);
      latch.countDown();
      return CompletableFuture.completedFuture(null);
    });
    latch.await(30, TimeUnit.SECONDS);
  }

  /**
   * Tests that the cluster leader is set when transitioning to leader.
   */
  public void testLeaderTakesLeadershipOnTransition() throws Throwable {
    CopycatStateContext context = createContext();
    context.open().get();
    context.pingHandler(request -> CompletableFuture.completedFuture(null));
    context.transition(CopycatState.LEADER).get();
    assertEquals(context.getLeader(), "local://foo");
  }

}
