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
package net.kuujo.mimeo.impl;

import java.util.HashMap;
import java.util.Map;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.mimeo.Command;
import net.kuujo.mimeo.Command.Type;
import net.kuujo.mimeo.Function;
import net.kuujo.mimeo.Node;
import net.kuujo.mimeo.cluster.ClusterConfig;
import net.kuujo.mimeo.log.Log;
import net.kuujo.mimeo.replica.Replica;
import net.kuujo.mimeo.replica.impl.RaftReplica;
import net.kuujo.mimeo.state.StateMachine;

/**
 * A default node implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultNode implements Node {
  private final Replica replica;
  private ClusterConfig config;
  private Map<String, CommandInfo> commands = new HashMap<>();

  private final StateMachine stateMachine = new StateMachine() {
    @Override
    public JsonObject applyCommand(Command command) {
      if (commands.containsKey(command.command())) {
        return commands.get(command.command()).function.call(command);
      }
      return null;
    }
  };

  private static class CommandInfo {
    private final Command.Type type;
    private final Function<Command, JsonObject> function;

    private CommandInfo(Command.Type type, Function<Command, JsonObject> function) {
      this.type = type;
      this.function = function;
    }
  }

  public DefaultNode(String address, Vertx vertx) {
    replica = new RaftReplica(address, vertx, stateMachine);
  }

  public DefaultNode(String address, Vertx vertx, Log log) {
    replica = new RaftReplica(address, vertx, stateMachine, log);
  }

  @Override
  public String getAddress() {
    return replica.getAddress();
  }

  @Override
  public Node setAddress(String address) {
    replica.setAddress(address);
    return this;
  }

  @Override
  public Node setClusterConfig(ClusterConfig config) {
    this.config = config;
    return this;
  }

  @Override
  public ClusterConfig getClusterConfig() {
    return config;
  }

  @Override
  public long getElectionTimeout() {
    return replica.getElectionTimeout();
  }

  @Override
  public Node setElectionTimeout(long timeout) {
    replica.setElectionTimeout(timeout);
    return this;
  }

  @Override
  public long getHeartbeatInterval() {
    return replica.getHeartbeatInterval();
  }

  @Override
  public Node setHeartbeatInterval(long interval) {
    replica.setHeartbeatInterval(interval);
    return this;
  }

  @Override
  public boolean isUseAdaptiveTimeouts() {
    return replica.isUseAdaptiveTimeouts();
  }

  @Override
  public Node useAdaptiveTimeouts(boolean useAdaptive) {
    replica.useAdaptiveTimeouts(useAdaptive);
    return this;
  }

  @Override
  public double getAdaptiveTimeoutThreshold() {
    return replica.getAdaptiveTimeoutThreshold();
  }

  @Override
  public Node setAdaptiveTimeoutThreshold(double threshold) {
    replica.setAdaptiveTimeoutThreshold(threshold);
    return this;
  }

  @Override
  public boolean isRequireWriteMajority() {
    return replica.isRequireWriteMajority();
  }

  @Override
  public Node setRequireWriteMajority(boolean require) {
    replica.setRequireWriteMajority(require);
    return this;
  }

  @Override
  public boolean isRequireReadMajority() {
    return replica.isRequireReadMajority();
  }

  @Override
  public Node setRequireReadMajority(boolean require) {
    replica.setRequireReadMajority(require);
    return this;
  }

  @Override
  public Node setLog(Log log) {
    replica.setLog(log);
    return this;
  }

  @Override
  public Log getLog() {
    return replica.getLog();
  }

  @Override
  public Node start() {
    replica.start();
    return this;
  }

  @Override
  public Node start(Handler<AsyncResult<Void>> doneHandler) {
    replica.start(doneHandler);
    return this;
  }

  @Override
  public Node registerCommand(String commandName, Function<Command, JsonObject> function) {
    commands.put(commandName, new CommandInfo(null, function));
    return this;
  }

  @Override
  public Node registerCommand(String commandName, Type type, Function<Command, JsonObject> function) {
    commands.put(commandName, new CommandInfo(type, function));
    return this;
  }

  @Override
  public Node unregisterCommand(String commandName) {
    commands.remove(commandName);
    return this;
  }

  @Override
  public Node submitCommand(String command, JsonObject data, Handler<AsyncResult<JsonObject>> resultHandler) {
    replica.submitCommand(new DefaultCommand(command, commands.containsKey(command) ? commands.get(command).type : null, data), resultHandler);
    return this;
  }

  @Override
  public void stop() {
    replica.stop();
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    replica.stop(doneHandler);
  }

}
