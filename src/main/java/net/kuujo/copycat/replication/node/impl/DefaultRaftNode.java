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
package net.kuujo.copycat.replication.node.impl;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.MemoryLog;
import net.kuujo.copycat.replication.StateMachine;
import net.kuujo.copycat.replication.node.RaftClient;
import net.kuujo.copycat.replication.node.RaftNode;
import net.kuujo.copycat.replication.protocol.SubmitRequest;
import net.kuujo.copycat.replication.protocol.SubmitResponse;
import net.kuujo.copycat.replication.state.StateContext;
import net.kuujo.copycat.replication.state.StateType;

/**
 * A Raft replica implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultRaftNode implements RaftNode {
  private final RaftClient endpoint;
  private final StateContext context;
  private ClusterConfig config = new ClusterConfig();

  public DefaultRaftNode(String address, Vertx vertx, StateMachine stateMachine) {
    endpoint = new DefaultRaftClient(address, vertx);
    context = new StateContext(vertx, endpoint, stateMachine, new MemoryLog());
  }

  public DefaultRaftNode(String address, Vertx vertx, StateMachine stateMachine, Log log) {
    endpoint = new DefaultRaftClient(address, vertx);
    context = new StateContext(vertx, endpoint, stateMachine, log);
  }

  @Override
  public RaftNode setAddress(String address) {
    endpoint.setAddress(address);
    return this;
  }

  @Override
  public String getAddress() {
    return endpoint.getAddress();
  }

  @Override
  public RaftNode setClusterConfig(ClusterConfig config) {
    this.config = config;
    return this;
  }

  @Override
  public ClusterConfig getClusterConfig() {
    return config;
  }

  @Override
  public RaftNode setElectionTimeout(long timeout) {
    context.electionTimeout(timeout);
    return this;
  }

  @Override
  public long getElectionTimeout() {
    return context.electionTimeout();
  }

  @Override
  public RaftNode setHeartbeatInterval(long interval) {
    context.heartbeatInterval(interval);
    return this;
  }

  @Override
  public long getHeartbeatInterval() {
    return context.heartbeatInterval();
  }

  @Override
  public boolean isUseAdaptiveTimeouts() {
    return context.useAdaptiveTimeouts();
  }

  @Override
  public RaftNode useAdaptiveTimeouts(boolean useAdaptive) {
    context.useAdaptiveTimeouts(useAdaptive);
    return this;
  }

  @Override
  public double getAdaptiveTimeoutThreshold() {
    return context.adaptiveTimeoutThreshold();
  }

  @Override
  public RaftNode setAdaptiveTimeoutThreshold(double threshold) {
    context.adaptiveTimeoutThreshold(threshold);
    return this;
  }

  @Override
  public boolean isRequireWriteMajority() {
    return context.requireWriteMajority();
  }

  @Override
  public RaftNode setRequireWriteMajority(boolean require) {
    context.requireWriteMajority(require);
    return this;
  }

  @Override
  public boolean isRequireReadMajority() {
    return context.requireReadMajority();
  }

  @Override
  public RaftNode setRequireReadMajority(boolean require) {
    context.requireReadMajority(require);
    return this;
  }

  @Override
  public RaftNode setStateMachine(StateMachine stateMachine) {
    context.stateMachine(stateMachine);
    return this;
  }

  @Override
  public StateMachine getStateMachine() {
    return context.stateMachine();
  }

  @Override
  public RaftNode setLog(Log log) {
    context.log(log);
    return this;
  }

  @Override
  public Log getLog() {
    return context.log();
  }

  @Override
  public RaftNode start() {
    context.configure(config);
    context.transition(StateType.START);
    endpoint.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.succeeded()) {
          context.transition(StateType.FOLLOWER);
        }
      }
    });
    return this;
  }

  @Override
  public RaftNode start(Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    context.configure(config);
    context.transition(StateType.START);
    endpoint.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          context.transition(StateType.FOLLOWER, new Handler<String>() {
            @Override
            public void handle(String leaderAddress) {
              future.setResult((Void) null);
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public <R> RaftNode submitCommand(Command command, Handler<AsyncResult<R>> doneHandler) {
    final Future<R> future = new DefaultFutureResult<R>().setHandler(doneHandler);
    endpoint.submit(context.currentLeader(), new SubmitRequest(command), new Handler<AsyncResult<SubmitResponse>>() {
      @Override
      @SuppressWarnings("unchecked")
      public void handle(AsyncResult<SubmitResponse> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          future.setResult((R) result.result().result());
        }
      }
    });
    return this;
  }

  @Override
  public void stop() {
    endpoint.stop();
    context.transition(StateType.START);
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    endpoint.stop(doneHandler);
    context.transition(StateType.START);
    new DefaultFutureResult<Void>().setHandler(doneHandler).setResult((Void) null);
  }

}
