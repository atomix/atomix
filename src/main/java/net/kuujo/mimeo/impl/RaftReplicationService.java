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
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.mimeo.Command;
import net.kuujo.mimeo.Command.Type;
import net.kuujo.mimeo.Function;
import net.kuujo.mimeo.MimeoException;
import net.kuujo.mimeo.ReplicationService;
import net.kuujo.mimeo.ReplicationServiceEndpoint;
import net.kuujo.mimeo.StateMachine;
import net.kuujo.mimeo.cluster.config.ClusterConfig;
import net.kuujo.mimeo.log.Log;
import net.kuujo.mimeo.log.MemoryLog;
import net.kuujo.mimeo.protocol.SubmitRequest;
import net.kuujo.mimeo.protocol.SubmitResponse;
import net.kuujo.mimeo.state.StateContext;
import net.kuujo.mimeo.state.StateType;

/**
 * A default replication service implementation.
 *
 * @author Jordan Halterman
 */
public class RaftReplicationService implements ReplicationService {
  private final String address;
  private final Vertx vertx;
  private final ReplicationServiceEndpoint endpoint;
  private final StateContext context;
  private Map<String, CommandInfo> commands = new HashMap<>();
  private long startTimer;
  private final StateMachine stateMachine;

  private static class CommandInfo {
    private final Command.Type type;
    private final Function<Command, JsonObject> function;
    private CommandInfo(Command.Type type, Function<Command, JsonObject> function) {
      this.type = type;
      this.function = function;
    }
  }

  public RaftReplicationService(String address, Vertx vertx) {
    this.address = address;
    this.vertx = vertx;
    this.stateMachine = createStateMachine();
    endpoint = new RaftReplicationServiceEndpoint(address, vertx);
    context = new StateContext(address, vertx, endpoint, new MemoryLog(), stateMachine);
  }

  public RaftReplicationService(String address, Vertx vertx, StateMachine stateMachine) {
    this(address, vertx, stateMachine, new MemoryLog());
  }

  public RaftReplicationService(String address, Vertx vertx, Log log) {
    this.address = address;
    this.vertx = vertx;
    this.stateMachine = createStateMachine();
    endpoint = new RaftReplicationServiceEndpoint(address, vertx);
    context = new StateContext(address, vertx, endpoint, log, stateMachine);
  }

  public RaftReplicationService(String address, Vertx vertx, StateMachine stateMachine, Log log) {
    this.address = address;
    this.vertx = vertx;
    this.stateMachine = stateMachine;
    endpoint = new RaftReplicationServiceEndpoint(address, vertx);
    context = new StateContext(address, vertx, endpoint, log, stateMachine);
  }

  private StateMachine createStateMachine() {
    return new StateMachine() {
      @Override
      public JsonObject applyCommand(Command command) {
        if (commands.containsKey(command.command())) {
          return commands.get(command.command()).function.call(command);
        }
        return null;
      }
    };
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public long getElectionTimeout() {
    return context.electionTimeout();
  }

  @Override
  public ReplicationService setElectionTimeout(long timeout) {
    context.electionTimeout(timeout);
    return this;
  }

  @Override
  public long getHeartbeatInterval() {
    return context.heartbeatInterval();
  }

  @Override
  public ReplicationService setHeartbeatInterval(long interval) {
    context.heartbeatInterval(interval);
    return this;
  }

  @Override
  public boolean isUseAdaptiveTimeouts() {
    return context.useAdaptiveTimeouts();
  }

  @Override
  public ReplicationService useAdaptiveTimeouts(boolean useAdaptive) {
    context.useAdaptiveTimeouts(useAdaptive);
    return this;
  }

  @Override
  public double getAdaptiveTimeoutThreshold() {
    return context.adaptiveTimeoutThreshold();
  }

  @Override
  public ReplicationService setAdaptiveTimeoutThreshold(double threshold) {
    context.adaptiveTimeoutThreshold(threshold);
    return this;
  }

  @Override
  public boolean isRequireWriteMajority() {
    return context.requireWriteMajority();
  }

  @Override
  public ReplicationService setRequireWriteMajority(boolean require) {
    context.requireWriteMajority(require);
    return this;
  }

  @Override
  public boolean isRequireReadMajority() {
    return context.requireReadMajority();
  }

  @Override
  public ReplicationService setRequireReadMajority(boolean require) {
    context.requireReadMajority(require);
    return this;
  }

  @Override
  public ReplicationService start(ClusterConfig config) {
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
  public ReplicationService start(ClusterConfig config, Handler<AsyncResult<Void>> doneHandler) {
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
  public ReplicationService registerCommand(String commandName, Type type, Function<Command, JsonObject> function) {
    commands.put(commandName, new CommandInfo(type, function));
    return this;
  }

  @Override
  public ReplicationService unregisterCommand(String commandName) {
    commands.remove(commandName);
    return this;
  }

  @Override
  public ReplicationService submitCommand(String command, JsonObject data, Handler<AsyncResult<JsonObject>> doneHandler) {
    final Future<JsonObject> future = new DefaultFutureResult<JsonObject>().setHandler(doneHandler);
    if (commands.containsKey(command)) {
      endpoint.submit(context.currentLeader(), new SubmitRequest(new DefaultCommand(command, commands.get(command).type, data)), new Handler<AsyncResult<SubmitResponse>>() {
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
    }
    else {
      future.setFailure(new MimeoException("Invalid command."));
    }
    return this;
  }

  @Override
  public void stop() {
    endpoint.stop();
    context.transition(StateType.START);
    if (startTimer > 0) {
      vertx.cancelTimer(startTimer);
      startTimer = 0;
    }
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    endpoint.stop(doneHandler);
    context.transition(StateType.START);
    if (startTimer > 0) {
      vertx.cancelTimer(startTimer);
      startTimer = 0;
    }
  }

}
