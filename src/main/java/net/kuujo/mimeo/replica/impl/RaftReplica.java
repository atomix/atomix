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
package net.kuujo.mimeo.replica.impl;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.mimeo.Command;
import net.kuujo.mimeo.cluster.ClusterConfig;
import net.kuujo.mimeo.log.Log;
import net.kuujo.mimeo.log.MemoryLog;
import net.kuujo.mimeo.protocol.SubmitRequest;
import net.kuujo.mimeo.protocol.SubmitResponse;
import net.kuujo.mimeo.replica.Replica;
import net.kuujo.mimeo.replica.ReplicaEndpoint;
import net.kuujo.mimeo.state.StateContext;
import net.kuujo.mimeo.state.StateMachine;
import net.kuujo.mimeo.state.StateType;

/**
 * A Raft replica implementation.
 *
 * @author Jordan Halterman
 */
public class RaftReplica implements Replica {
  private final ReplicaEndpoint endpoint;
  private final StateContext context;
  private ClusterConfig config;

  public RaftReplica(String address, Vertx vertx, StateMachine stateMachine) {
    endpoint = new RaftReplicaEndpoint(address, vertx);
    context = new StateContext(vertx, endpoint, stateMachine, new MemoryLog());
  }

  public RaftReplica(String address, Vertx vertx, StateMachine stateMachine, Log log) {
    endpoint = new RaftReplicaEndpoint(address, vertx);
    context = new StateContext(vertx, endpoint, stateMachine, log);
  }

  @Override
  public Replica setAddress(String address) {
    endpoint.setAddress(address);
    return this;
  }

  @Override
  public String getAddress() {
    return endpoint.getAddress();
  }

  @Override
  public Replica setClusterConfig(ClusterConfig config) {
    this.config = config;
    return this;
  }

  @Override
  public ClusterConfig getClusterConfig() {
    return config;
  }

  @Override
  public Replica setElectionTimeout(long timeout) {
    context.electionTimeout(timeout);
    return this;
  }

  @Override
  public long getElectionTimeout() {
    return context.electionTimeout();
  }

  @Override
  public Replica setHeartbeatInterval(long interval) {
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
  public Replica useAdaptiveTimeouts(boolean useAdaptive) {
    context.useAdaptiveTimeouts(useAdaptive);
    return this;
  }

  @Override
  public double getAdaptiveTimeoutThreshold() {
    return context.adaptiveTimeoutThreshold();
  }

  @Override
  public Replica setAdaptiveTimeoutThreshold(double threshold) {
    context.adaptiveTimeoutThreshold(threshold);
    return this;
  }

  @Override
  public boolean isRequireWriteMajority() {
    return context.requireWriteMajority();
  }

  @Override
  public Replica setRequireWriteMajority(boolean require) {
    context.requireWriteMajority(require);
    return this;
  }

  @Override
  public boolean isRequireReadMajority() {
    return context.requireReadMajority();
  }

  @Override
  public Replica setRequireReadMajority(boolean require) {
    context.requireReadMajority(require);
    return this;
  }

  @Override
  public Replica setStateMachine(StateMachine stateMachine) {
    context.stateMachine(stateMachine);
    return this;
  }

  @Override
  public StateMachine getStateMachine() {
    return context.stateMachine();
  }

  @Override
  public Replica setLog(Log log) {
    context.log(log);
    return this;
  }

  @Override
  public Log getLog() {
    return context.log();
  }

  @Override
  public Replica start() {
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
  public Replica start(Handler<AsyncResult<Void>> doneHandler) {
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
  public Replica submitCommand(Command command, Handler<AsyncResult<JsonObject>> doneHandler) {
    final Future<JsonObject> future = new DefaultFutureResult<JsonObject>().setHandler(doneHandler);
    endpoint.submit(context.currentLeader(), new SubmitRequest(command), new Handler<AsyncResult<SubmitResponse>>() {
      @Override
      public void handle(AsyncResult<SubmitResponse> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          future.setResult(result.result().result());
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
