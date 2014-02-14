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
import org.vertx.java.platform.Container;
import org.vertx.java.platform.Verticle;

import net.kuujo.copycat.ClusterConfig;
import net.kuujo.copycat.Replica;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.state.StateContext;
import net.kuujo.copycat.state.StateType;

/**
 * A default replica implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultReplica implements Replica {
  private final StateContext context;

  public DefaultReplica(String address, Verticle verticle, StateMachine stateMachine) {
    this(address, verticle.getVertx(), verticle.getContainer(), stateMachine);
  }

  public DefaultReplica(String address, Vertx vertx, Container container, StateMachine stateMachine) {
    context = new StateContext(address, vertx, container, new DefaultStateMachineExecutor(stateMachine));
  }

  public DefaultReplica(String address, Verticle verticle, StateMachine stateMachine, ClusterConfig config) {
    this(address, verticle, stateMachine);
    context.configure(config);
  }

  public DefaultReplica(String address, Vertx vertx, Container container, StateMachine stateMachine, ClusterConfig config) {
    this(address, vertx, container, stateMachine);
    context.configure(config);
  }

  @Override
  public String address() {
    return context.address();
  }

  @Override
  public ClusterConfig config() {
    return context.config();
  }

  @Override
  public Replica setLogType(Log.Type type) {
    context.log().setLogType(type);
    return this;
  }

  @Override
  public Log.Type getLogType() {
    return context.log().getLogType();
  }

  @Override
  public Replica setLogFile(String filename) {
    context.log().setLogFile(filename);
    return this;
  }

  @Override
  public String getLogFile() {
    return context.log().getLogFile();
  }

  @Override
  public Replica setMaxLogSize(long max) {
    context.log().setMaxSize(max);
    return this;
  }

  @Override
  public long getMaxLogSize() {
    return context.log().getMaxSize();
  }

  @Override
  public long getElectionTimeout() {
    return context.electionTimeout();
  }

  @Override
  public Replica setElectionTimeout(long timeout) {
    context.electionTimeout(timeout);
    return this;
  }

  @Override
  public long getHeartbeatInterval() {
    return context.heartbeatInterval();
  }

  @Override
  public Replica setHeartbeatInterval(long interval) {
    context.heartbeatInterval(interval);
    return this;
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
  public boolean isLeader() {
    return context.currentState().equals(StateType.LEADER);
  }

  @Override
  public String getCurrentLeader() {
    return context.currentLeader();
  }

  @Override
  public Replica start() {
    context.start();
    return this;
  }

  @Override
  public Replica start(Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    context.start(new Handler<AsyncResult<String>>() {
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
    context.submitCommand(command, args, resultHandler);
    return this;
  }

  @Override
  public void stop() {
    context.stop();
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    context.stop(doneHandler);
  }

}
