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
package net.kuujo.copycat.impl;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.copycat.ClusterConfig;
import net.kuujo.copycat.Replica;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.MemoryLog;
import net.kuujo.copycat.state.StateContext;
import net.kuujo.copycat.state.StateType;

/**
 * A default replica implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultReplica implements Replica {
  private final StateContext state;

  public DefaultReplica(String address, Vertx vertx, StateMachine stateMachine) {
    state = new StateContext(address, vertx, stateMachine, new MemoryLog());
  }

  public DefaultReplica(String address, Vertx vertx, StateMachine stateMachine, Log log) {
    state = new StateContext(address, vertx, stateMachine, log);
  }

  @Override
  public String address() {
    return state.address();
  }

  @Override
  public ClusterConfig config() {
    return state.config();
  }

  @Override
  public Log log() {
    return state.log();
  }

  @Override
  public Replica setMaxMemoryUsage(long max) {
    state.maxMemoryUsage(max);
    return this;
  }

  @Override
  public long getMaxMemoryUsage() {
    return state.maxMemoryUsage();
  }

  @Override
  public long getElectionTimeout() {
    return state.electionTimeout();
  }

  @Override
  public Replica setElectionTimeout(long timeout) {
    state.electionTimeout(timeout);
    return this;
  }

  @Override
  public long getHeartbeatInterval() {
    return state.heartbeatInterval();
  }

  @Override
  public Replica setHeartbeatInterval(long interval) {
    state.heartbeatInterval(interval);
    return this;
  }

  @Override
  public boolean isUseAdaptiveTimeouts() {
    return state.useAdaptiveTimeouts();
  }

  @Override
  public Replica useAdaptiveTimeouts(boolean useAdaptive) {
    state.useAdaptiveTimeouts(useAdaptive);
    return this;
  }

  @Override
  public double getAdaptiveTimeoutThreshold() {
    return state.adaptiveTimeoutThreshold();
  }

  @Override
  public Replica setAdaptiveTimeoutThreshold(double threshold) {
    state.adaptiveTimeoutThreshold(threshold);
    return this;
  }

  @Override
  public boolean isRequireWriteMajority() {
    return state.requireWriteMajority();
  }

  @Override
  public Replica setRequireWriteMajority(boolean require) {
    state.requireWriteMajority(require);
    return this;
  }

  @Override
  public boolean isRequireReadMajority() {
    return state.requireReadMajority();
  }

  @Override
  public Replica setRequireReadMajority(boolean require) {
    state.requireReadMajority(require);
    return this;
  }

  @Override
  public Replica transitionHandler(Handler<StateType> handler) {
    state.transitionHandler(handler);
    return this;
  }

  @Override
  public boolean isFollower() {
    return state.currentState().equals(StateType.FOLLOWER);
  }

  @Override
  public boolean isCandidate() {
    return state.currentState().equals(StateType.CANDIDATE);
  }

  @Override
  public boolean isLeader() {
    return state.currentState().equals(StateType.LEADER);
  }

  @Override
  public String getCurrentLeader() {
    return state.currentLeader();
  }

  @Override
  public Replica start() {
    state.start();
    return this;
  }

  @Override
  public Replica start(Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    state.start(new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          future.setResult((Void) null);
        }
      }
    });
    return this;
  }

  @Override
  public <R> Replica submitCommand(String command, JsonObject args, Handler<AsyncResult<R>> resultHandler) {
    state.submitCommand(command, args, resultHandler);
    return this;
  }

  @Override
  public void stop() {
    state.stop();
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    state.stop(doneHandler);
  }

}
