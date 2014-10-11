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

import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.state.*;
import net.kuujo.copycat.spi.protocol.AsyncProtocol;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test node.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestNode {
  private final TestNodeEvents events;
  private StateContext context;
  private String id;
  private Member member;
  private CopycatState state;
  private TestStateMachine stateMachine;
  private TestLog log;
  private String leader;
  private long term;
  private String votedFor;
  private long commitIndex;
  private long lastApplied;

  public TestNode(String id) {
    this.id = id;
    this.member = new Member(id);
    this.events = new TestNodeEvents(this);
  }

  /**
   * Returns the node id.
   *
   * @return The node id.
   */
  public String id() {
    return id;
  }

  /**
   * Returns the node configuration.
   *
   * @return The node configuration.
   */
  public Member member() {
    return member;
  }

  /**
   * Sets the initial node state.
   *
   * @param state The initial node state.
   * @return The test node.
   */
  public TestNode withState(CopycatState state) {
    this.state = state;
    return this;
  }

  /**
   * Sets the test state machine.
   *
   * @param stateMachine The test state machine.
   * @return The test node.
   */
  public TestNode withStateMachine(TestStateMachine stateMachine) {
    this.stateMachine = stateMachine;
    return this;
  }

  /**
   * Sets the test log.
   *
   * @param log The test log.
   * @return The test node.
   */
  public TestNode withLog(TestLog log) {
    this.log = log;
    return this;
  }

  /**
   * Sets the initial node leader.
   *
   * @param leader The initial leader.
   * @return The test node.
   */
  public TestNode withLeader(String leader) {
    this.leader = leader;
    return this;
  }

  /**
   * Sets the initial node term.
   *
   * @param term The initial node term.
   * @return The test node.
   */
  public TestNode withTerm(long term) {
    this.term = term;
    return this;
  }

  /**
   * Sets the initial last candidate voted for by the node.
   *
   * @param candidate The last candidate voted for.
   * @return The test node.
   */
  public TestNode withVotedFor(String candidate) {
    this.votedFor = candidate;
    return this;
  }

  /**
   * Sets the initial commit index.
   *
   * @param commitIndex The initial commit index.
   * @return The test node.
   */
  public TestNode withCommitIndex(long commitIndex) {
    this.commitIndex = commitIndex;
    return this;
  }

  /**
   * Sets the initial last index applied.
   *
   * @param lastApplied The initial last index applied to the state machine.
   * @return The test node.
   */
  public TestNode withLastApplied(long lastApplied) {
    this.lastApplied = lastApplied;
    return this;
  }

  /**
   * Returns the test state machine.
   */
  public TestStateMachine stateMachine() {
    return stateMachine;
  }

  /**
   * Returns the test log.
   */
  public TestLog log() {
    return log;
  }

  /**
   * Returns test event listeners.
   */
  public TestNodeEvents await() {
    return events;
  }

  /**
   * Returns the node context.
   */
  public StateContext instance() {
    return context;
  }

  /**
   * Starts the node.
   *
   * @param cluster The cluster configuration.
   */
  public <M extends Member> void start(Cluster<M> cluster, AsyncProtocol<M> protocol) {
    context = new StateContext(stateMachine, log, cluster, protocol, new CopycatConfig());
    context.currentLeader(leader);
    context.currentTerm(term);
    context.lastVotedFor(votedFor);
    context.commitIndex(commitIndex);
    context.lastApplied(lastApplied);
    switch (state) {
      case NONE:
        context.transition(NoneController.class);
        break;
      case FOLLOWER:
        context.transition(FollowerController.class);
        break;
      case CANDIDATE:
        context.transition(CandidateController.class);
        break;
      case LEADER:
        context.transition(LeaderController.class);
        break;
    }

    final CountDownLatch latch = new CountDownLatch(1);
    context.clusterManager().localNode().server().listen().whenCompleteAsync((result, error) -> latch.countDown());
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
