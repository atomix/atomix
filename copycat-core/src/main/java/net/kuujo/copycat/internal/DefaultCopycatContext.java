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

import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.event.*;
import net.kuujo.copycat.internal.event.DefaultEvents;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.log.Log;

import java.util.concurrent.CompletableFuture;

/**
 * CopyCat replica context.<p>
 *
 * The <code>CopyCatContext</code> is the primary API for creating
 * and running a CopyCat replica. Given a state machine, a cluster
 * configuration, and a log, the context will communicate with other
 * nodes in the cluster, applying and replicating state machine commands.<p>
 *
 * CopyCat uses a Raft-based consensus algorithm to perform leader election
 * and state machine replication. In CopyCat, all state changes are made
 * through the cluster leader. When a cluster is started, nodes will
 * communicate with one another to elect a leader. When a command is submitted
 * to any node in the cluster, the command will be forwarded to the leader.
 * When the leader receives a command submission, it will first replicate
 * the command to its followers before applying the command to its state
 * machine and returning the result.<p>
 *
 * In order to prevent logs from growing too large, CopyCat uses snapshotting
 * to periodically compact logs. In CopyCat, snapshots are simply log
 * entries before which all previous entries are cleared. When a node first
 * becomes the cluster leader, it will first commit a snapshot of its current
 * state to its log. This snapshot can be used to get any new nodes up to date.<p>
 *
 * CopyCat supports dynamic cluster membership changes. If the {@link Cluster}
 * provided to the CopyCat context is {@link java.util.Observable}, the cluster
 * leader will observe the configuration for changes. Note that cluster membership
 * changes can only occur on the leader's cluster configuration. This is because,
 * as with all state changes, cluster membership changes must go through the leader.
 * When cluster membership changes occur, the cluster leader will log and replicate
 * the configuration change just like any other state change, and it will ensure
 * that the membership change occurs in a manner that prevents a dual-majority
 * in the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycatContext implements CopycatContext {
  private final Cluster<?> cluster;
  private final CopycatConfig config;
  private final StateContext state;
  private final Events events;

  <M extends Member> DefaultCopycatContext(StateMachine stateMachine, Log log, Cluster<M> cluster, CopycatConfig config) {
    this.cluster = cluster;
    this.config = config;
    this.state = new StateContext(stateMachine, log, cluster, config);
    this.events = new DefaultEvents(state.events());
  }

  @Override
  public CopycatConfig config() {
    return config;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <M extends Member> Cluster<M> cluster() {
    return (Cluster<M>) cluster;
  }

  @Override
  public Events on() {
    return events;
  }

  @Override
  public <T extends Event> EventContext<T> on(Class<T> event) {
    return events.event(event);
  }

  @Override
  public EventHandlers events() {
    return state.events();
  }

  @Override
  public <T extends Event> EventHandlerRegistry<T> event(Class<T> event) {
    return state.events().event(event);
  }

  @Override
  public CopycatState state() {
    return state.state();
  }

  @Override
  public String leader() {
    return state.currentLeader();
  }

  @Override
  public boolean isLeader() {
    return state.isLeader();
  }

  @Override
  public CompletableFuture<Void> start() {
    return state.start();
  }

  @Override
  public CompletableFuture<Void> stop() {
    return state.stop();
  }

  @Override
  public <R> CompletableFuture<R> submitCommand(final String command, final Object... args) {
    return state.submitCommand(command, args);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
