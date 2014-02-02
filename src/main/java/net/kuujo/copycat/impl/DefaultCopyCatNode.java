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
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.Function;
import net.kuujo.copycat.CopyCatNode;
import net.kuujo.copycat.Command.Type;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.replica.Replica;
import net.kuujo.copycat.replica.impl.RaftReplica;
import net.kuujo.copycat.state.StateMachine;

/**
 * A default node implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultCopyCatNode implements CopyCatNode {
  private final Replica replica;
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

  public DefaultCopyCatNode(Vertx vertx) {
    replica = new RaftReplica(UUID.randomUUID().toString(), vertx, stateMachine);
  }

  public DefaultCopyCatNode(String address, Vertx vertx) {
    replica = new RaftReplica(address, vertx, stateMachine);
  }

  public DefaultCopyCatNode(String address, Vertx vertx, Log log) {
    replica = new RaftReplica(address, vertx, stateMachine, log);
  }

  @Override
  public String getAddress() {
    return replica.getAddress();
  }

  @Override
  public CopyCatNode setAddress(String address) {
    replica.setAddress(address);
    return this;
  }

  @Override
  public ClusterConfig cluster() {
    return replica.getClusterConfig();
  }

  @Override
  public CopyCatNode setClusterConfig(ClusterConfig config) {
    replica.setClusterConfig(config);
    return this;
  }

  @Override
  public ClusterConfig getClusterConfig() {
    return replica.getClusterConfig();
  }

  @Override
  public long getElectionTimeout() {
    return replica.getElectionTimeout();
  }

  @Override
  public CopyCatNode setElectionTimeout(long timeout) {
    replica.setElectionTimeout(timeout);
    return this;
  }

  @Override
  public long getHeartbeatInterval() {
    return replica.getHeartbeatInterval();
  }

  @Override
  public CopyCatNode setHeartbeatInterval(long interval) {
    replica.setHeartbeatInterval(interval);
    return this;
  }

  @Override
  public boolean isUseAdaptiveTimeouts() {
    return replica.isUseAdaptiveTimeouts();
  }

  @Override
  public CopyCatNode useAdaptiveTimeouts(boolean useAdaptive) {
    replica.useAdaptiveTimeouts(useAdaptive);
    return this;
  }

  @Override
  public double getAdaptiveTimeoutThreshold() {
    return replica.getAdaptiveTimeoutThreshold();
  }

  @Override
  public CopyCatNode setAdaptiveTimeoutThreshold(double threshold) {
    replica.setAdaptiveTimeoutThreshold(threshold);
    return this;
  }

  @Override
  public boolean isRequireWriteMajority() {
    return replica.isRequireWriteMajority();
  }

  @Override
  public CopyCatNode setRequireWriteMajority(boolean require) {
    replica.setRequireWriteMajority(require);
    return this;
  }

  @Override
  public boolean isRequireReadMajority() {
    return replica.isRequireReadMajority();
  }

  @Override
  public CopyCatNode setRequireReadMajority(boolean require) {
    replica.setRequireReadMajority(require);
    return this;
  }

  @Override
  public CopyCatNode setLog(Log log) {
    replica.setLog(log);
    return this;
  }

  @Override
  public Log getLog() {
    return replica.getLog();
  }

  @Override
  public CopyCatNode start() {
    replica.start();
    return this;
  }

  @Override
  public CopyCatNode start(Handler<AsyncResult<Void>> doneHandler) {
    replica.start(doneHandler);
    return this;
  }

  @Override
  public <R> CopyCatNode registerCommand(String commandName, Function<Command, R> function) {
    commands.put(commandName, new CommandInfo<R>(null, function));
    return this;
  }

  @Override
  public <R> CopyCatNode registerCommand(String commandName, Type type, Function<Command, R> function) {
    commands.put(commandName, new CommandInfo<R>(type, function));
    return this;
  }

  @Override
  public CopyCatNode unregisterCommand(String commandName) {
    commands.remove(commandName);
    return this;
  }

  @Override
  public <R> CopyCatNode submitCommand(String command, JsonObject data, Handler<AsyncResult<R>> resultHandler) {
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
