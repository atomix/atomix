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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.kuujo.copycat.CopyCatConfig;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.state.State;
import net.kuujo.copycat.state.impl.Candidate;
import net.kuujo.copycat.state.impl.Follower;
import net.kuujo.copycat.state.impl.Leader;
import net.kuujo.copycat.state.impl.None;
import net.kuujo.copycat.state.impl.RaftStateContext;

/**
 * Test node.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestNode {
  private final TestNodeEvents events;
  private RaftStateContext context;
  private String uri;
  private State.Type state;
  private TestStateMachine stateMachine;
  private TestLog log;
  private String leader;
  private long term;
  private String votedFor;
  private long commitIndex;
  private long lastApplied;

  public TestNode(String uri) {
    this.uri = uri;
    this.events = new TestNodeEvents(this);
  }

  /**
   * Returns the node URI.
   *
   * @return The node URI.
   */
  public String uri() {
    return uri;
  }

  /**
   * Sets the initial node state.
   *
   * @param state The initial node state.
   * @return The test node.
   */
  public TestNode withState(State.Type state) {
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
  public RaftStateContext instance() {
    return context;
  }

  /**
   * Starts the node.
   *
   * @param cluster The cluster configuration.
   * @param registry The cluster-wide registry.
   */
  public void start(ClusterConfig cluster, Registry registry) {
    context = new RaftStateContext(stateMachine, log, cluster, new CopyCatConfig(), registry);
    context.setCurrentLeader(leader);
    context.setCurrentTerm(term);
    context.setLastVotedFor(votedFor);
    context.setCommitIndex(commitIndex);
    context.setLastApplied(lastApplied);
    switch (state) {
      case NONE:
        context.transition(None.class);
        break;
      case FOLLOWER:
        context.transition(Follower.class);
        break;
      case CANDIDATE:
        context.transition(Candidate.class);
        break;
      case LEADER:
        context.transition(Leader.class);
        break;
    }

    final CountDownLatch latch = new CountDownLatch(1);
    context.cluster().localMember().protocol().server().start().whenCompleteAsync((result, error) -> {
      latch.countDown();
    });
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
