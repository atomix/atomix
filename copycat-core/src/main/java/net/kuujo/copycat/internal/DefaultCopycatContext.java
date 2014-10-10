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
import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.internal.util.Args;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.spi.protocol.Protocol;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
public class DefaultCopycatContext extends AbstractCopycatContext implements CopycatContext {

  <M extends Member> DefaultCopycatContext(StateMachine stateMachine, Log log, Cluster<M> cluster, Protocol<M> protocol, CopycatConfig config) {
    super(new StateContext(stateMachine, log, cluster, protocol, config), cluster, config);
  }

  @Override
  public void start() {
    CountDownLatch latch = new CountDownLatch(1);
    state.start().thenRun(latch::countDown);
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new CopycatException(e);
    };
  }

  @Override
  public void stop() {
    CountDownLatch latch = new CountDownLatch(1);
    state.stop().thenRun(latch::countDown);
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new CopycatException(e);
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public <R> R submitCommand(final String command, final Object... args) {
    final CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<R> result = new AtomicReference<>();
    state.submitCommand(Args.checkNotNull(command, "command cannot be null"), args).whenComplete((r, error) -> {
      latch.countDown();
      result.set((R) r);
    });
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new CopycatException(e);
    }
    return result.get();
  }

  @Override
  public String toString() {
    return String.format("%s[state=%s]", getClass().getSimpleName(), state.state());
  }

}
