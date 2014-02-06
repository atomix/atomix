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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.Function;
import net.kuujo.copycat.Replica;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.Command.Type;
import net.kuujo.copycat.cluster.ClusterConfig;
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
  private Map<String, CommandInfo<?>> commands = new HashMap<>();

  private final StateMachine stateMachine = new StateMachine() {
    @Override
    public Object applyCommand(Command command) {
      if (commands.containsKey(command.command())) {
        return commands.get(command.command()).function.call(command);
      }
      return null;
    }
  };

  private static class CommandInfo<R> {
    private final Command.Type type;
    private final Function<Command, R> function;

    private CommandInfo(Command.Type type, Function<Command, R> function) {
      this.type = type;
      this.function = function;
    }
  }

  public DefaultReplica(Vertx vertx) {
    state = new StateContext(UUID.randomUUID().toString(), vertx, stateMachine, new MemoryLog());
  }

  public DefaultReplica(String address, Vertx vertx) {
    state = new StateContext(address, vertx, stateMachine, new MemoryLog());
  }

  public DefaultReplica(String address, Vertx vertx, Log log) {
    state = new StateContext(address, vertx, stateMachine, log);
  }

  @Override
  public String getAddress() {
    return state.address();
  }

  @Override
  public Replica setAddress(String address) {
    state.address(address);
    return this;
  }

  @Override
  public ClusterConfig cluster() {
    return state.config();
  }

  @Override
  public Replica setClusterConfig(ClusterConfig config) {
    state.configure(config);
    return this;
  }

  @Override
  public ClusterConfig getClusterConfig() {
    return state.config();
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
  public Replica setLog(Log log) {
    state.log(log);
    return this;
  }

  @Override
  public Log getLog() {
    return state.log();
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
  public <R> Replica registerCommand(String commandName, Function<Command, R> function) {
    commands.put(commandName, new CommandInfo<R>(null, function));
    return this;
  }

  @Override
  public <R> Replica registerCommand(String commandName, Type type, Function<Command, R> function) {
    commands.put(commandName, new CommandInfo<R>(type, function));
    return this;
  }

  @Override
  public Replica unregisterCommand(String commandName) {
    commands.remove(commandName);
    return this;
  }

  @Override
  public <R> Replica submitCommand(String command, JsonObject data, Handler<AsyncResult<R>> resultHandler) {
    state.submitCommand(new DefaultCommand(command, commands.containsKey(command) ? commands.get(command).type : null, data), resultHandler);
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
