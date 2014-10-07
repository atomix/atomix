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
package net.kuujo.copycat.test;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.event.EventHandler;
import net.kuujo.copycat.event.LeaderElectEvent;
import net.kuujo.copycat.event.StateChangeEvent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test node events.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestNodeEvents {
  private final TestNode node;

  public TestNodeEvents(TestNode node) {
    this.node = node;
  }

  /**
   * Listens for the node to record a leader election.
   *
   * @return The events object.
   */
  public TestNodeEvents leaderElected() {
    final CountDownLatch latch = new CountDownLatch(1);
    node.instance().events().leaderElect().registerHandler(new EventHandler<LeaderElectEvent>() {
      @Override
      public void handle(LeaderElectEvent event) {
        node.instance().events().leaderElect().unregisterHandler(this);
        latch.countDown();
      }
    });
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  /**
   * Listens for the node to be elected leader.
   *
   * @return The events object.
   */
  public TestNodeEvents electedLeader() {
    final CountDownLatch latch = new CountDownLatch(1);
    node.instance().events().leaderElect().registerHandler(new EventHandler<LeaderElectEvent>() {
      @Override
      public void handle(LeaderElectEvent event) {
        if (event.leader().equals(node.id())) {
          node.instance().events().leaderElect().unregisterHandler(this);
          latch.countDown();
        }
      }
    });
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  /**
   * Listens for the node to transition to the given state.
   *
   * @param state The state for which to listen.
   * @return The events object.
   */
  public TestNodeEvents transition(final CopycatState state) {
    final CountDownLatch latch = new CountDownLatch(1);
    node.instance().events().stateChange().registerHandler(new EventHandler<StateChangeEvent>() {
      @Override
      public void handle(StateChangeEvent event) {
        if (event.state().equals(state)) {
          node.instance().events().stateChange().unregisterHandler(this);
          latch.countDown();
        }
      }
    });
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

}
