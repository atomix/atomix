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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterMember;
import net.kuujo.copycat.internal.state.CandidateController;
import net.kuujo.copycat.internal.state.FollowerController;
import net.kuujo.copycat.internal.state.LeaderController;
import net.kuujo.copycat.internal.state.NoneController;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.protocol.LocalProtocol;
import net.kuujo.copycat.spi.protocol.Protocol;

/**
 * Test node.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestNode {
  private final TestNodeEvents events;
  private StateContext context;
  private Cluster cluster;
  private Protocol protocol = new LocalProtocol();
  private CopycatConfig config = new CopycatConfig();
  private CopycatState state = CopycatState.FOLLOWER;
  private TestStateMachine stateMachine = new TestStateMachine();
  private TestLog log = new TestLog();
  private String leader;
  private long term;
  private String votedFor;
  private long commitIndex;
  private long lastApplied;

  public TestNode() {
    this.events = new TestNodeEvents(this);
  }

  /**
   * Sets the test node cluster.
   *
   * @param members The test node cluster members.
   * @return The test node.
   */
  public TestNode withCluster(String... members) {
    List<String> remoteMembers = new ArrayList<>();
    for (int i = 1; i < members.length; i++) {
      remoteMembers.add(members[i]);
    }
    this.cluster = new Cluster(members[0], remoteMembers);
    return this;
  }

  /**
   * Sets the test node cluster.
   *
   * @param cluster The test node cluster.
   * @return The test node.
   */
  public TestNode withCluster(Cluster cluster) {
    this.cluster = cluster;
    return this;
  }

  /**
   * Sets the test node protocol.
   *
   * @param protocol The test node protocol.
   * @return The test node.
   */
  public TestNode withProtocol(Protocol protocol) {
    this.protocol = protocol;
    return this;
  }

  /**
   * Returns the node id.
   *
   * @return The node id.
   */
  public String id() {
    return cluster.localMember().id();
  }

  /**
   * Returns the node configuration.
   *
   * @return The node configuration.
   */
  public ClusterMember member() {
    return cluster.localMember();
  }

  /**
   * Returns the node protocol.
   *
   * @return The node protocol.
   */
  public Protocol protocol() {
    return protocol;
  }

  /**
   * Kills the node.
   */
  public void kill() {
    if (context != null) {
      context.stop();
    }
  }

  /**
   * Sets the node configuration.
   *
   * @param config The node configuration.
   * @return The test node.
   */
  public TestNode withConfig(CopycatConfig config) {
    this.config = config;
    return this;
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
   */
  public void start() {
    context = new StateContext(stateMachine, log, cluster, protocol, config);
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

  /**
   * Stops the test node.
   */
  public void stop() {
    if (context != null) {
      CountDownLatch latch = new CountDownLatch(1);
      context.stop().thenRun(latch::countDown);
      try {
        latch.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
