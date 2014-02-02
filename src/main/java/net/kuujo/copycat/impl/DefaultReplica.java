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
import net.kuujo.copycat.Replica;
import net.kuujo.copycat.Command.Type;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.replication.StateMachine;
import net.kuujo.copycat.replication.node.RaftNode;
import net.kuujo.copycat.replication.node.impl.DefaultRaftNode;

/**
 * A default replica implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultReplica implements Replica {
  private final RaftNode node;
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
    node = new DefaultRaftNode(UUID.randomUUID().toString(), vertx, stateMachine);
  }

  public DefaultReplica(String address, Vertx vertx) {
    node = new DefaultRaftNode(address, vertx, stateMachine);
  }

  public DefaultReplica(String address, Vertx vertx, Log log) {
    node = new DefaultRaftNode(address, vertx, stateMachine, log);
  }

  @Override
  public String getAddress() {
    return node.getAddress();
  }

  @Override
  public Replica setAddress(String address) {
    node.setAddress(address);
    return this;
  }

  @Override
  public ClusterConfig cluster() {
    return node.getClusterConfig();
  }

  @Override
  public Replica setClusterConfig(ClusterConfig config) {
    node.setClusterConfig(config);
    return this;
  }

  @Override
  public ClusterConfig getClusterConfig() {
    return node.getClusterConfig();
  }

  @Override
  public long getElectionTimeout() {
    return node.getElectionTimeout();
  }

  @Override
  public Replica setElectionTimeout(long timeout) {
    node.setElectionTimeout(timeout);
    return this;
  }

  @Override
  public long getHeartbeatInterval() {
    return node.getHeartbeatInterval();
  }

  @Override
  public Replica setHeartbeatInterval(long interval) {
    node.setHeartbeatInterval(interval);
    return this;
  }

  @Override
  public boolean isUseAdaptiveTimeouts() {
    return node.isUseAdaptiveTimeouts();
  }

  @Override
  public Replica useAdaptiveTimeouts(boolean useAdaptive) {
    node.useAdaptiveTimeouts(useAdaptive);
    return this;
  }

  @Override
  public double getAdaptiveTimeoutThreshold() {
    return node.getAdaptiveTimeoutThreshold();
  }

  @Override
  public Replica setAdaptiveTimeoutThreshold(double threshold) {
    node.setAdaptiveTimeoutThreshold(threshold);
    return this;
  }

  @Override
  public boolean isRequireWriteMajority() {
    return node.isRequireWriteMajority();
  }

  @Override
  public Replica setRequireWriteMajority(boolean require) {
    node.setRequireWriteMajority(require);
    return this;
  }

  @Override
  public boolean isRequireReadMajority() {
    return node.isRequireReadMajority();
  }

  @Override
  public Replica setRequireReadMajority(boolean require) {
    node.setRequireReadMajority(require);
    return this;
  }

  @Override
  public Replica setLog(Log log) {
    node.setLog(log);
    return this;
  }

  @Override
  public Log getLog() {
    return node.getLog();
  }

  @Override
  public Replica start() {
    node.start();
    return this;
  }

  @Override
  public Replica start(Handler<AsyncResult<Void>> doneHandler) {
    node.start(doneHandler);
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
    node.submitCommand(new DefaultCommand(command, commands.containsKey(command) ? commands.get(command).type : null, data), resultHandler);
    return this;
  }

  @Override
  public void stop() {
    node.stop();
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    node.stop(doneHandler);
  }

}
