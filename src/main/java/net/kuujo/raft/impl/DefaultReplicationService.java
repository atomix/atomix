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
package net.kuujo.raft.impl;

import java.util.Set;
import java.util.UUID;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.raft.Command;
import net.kuujo.raft.Function;
import net.kuujo.raft.ReplicationService;
import net.kuujo.raft.ReplicationServiceEndpoint;
import net.kuujo.raft.StateMachine;
import net.kuujo.raft.log.impl.MemoryLog;
import net.kuujo.raft.protocol.SubmitRequest;
import net.kuujo.raft.protocol.SubmitResponse;
import net.kuujo.raft.state.StateContext;
import net.kuujo.raft.state.StateType;

public class DefaultReplicationService implements ReplicationService {
  private final String address;
  private final Vertx vertx;
  private final StateMachine stateMachine;
  private final ReplicationServiceEndpoint endpoint;
  private final StateContext context;
  private long startTimer;

  public DefaultReplicationService(Vertx vertx) {
    this(UUID.randomUUID().toString(), vertx, new DefaultStateMachine());
  }

  public DefaultReplicationService(String address, Vertx vertx) {
    this(address, vertx, new DefaultStateMachine());
  }

  public DefaultReplicationService(String address, Vertx vertx, StateMachine stateMachine) {
    this.address = address;
    this.vertx = vertx;
    this.stateMachine = stateMachine;
    endpoint = new DefaultReplicationServiceEndpoint(address, vertx);
    context = new StateContext(vertx, endpoint, new MemoryLog(), stateMachine);
    context.addMember(address);
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public ReplicationService setElectionTimeout(long timeout) {
    context.electionTimeout(timeout);
    return this;
  }

  @Override
  public long getElectionTimeout() {
    return context.electionTimeout();
  }

  @Override
  public boolean hasMember(String address) {
    return context.hasMember(address);
  }

  @Override
  public ReplicationService addMember(String address) {
    context.addMember(address);
    return this;
  }

  @Override
  public ReplicationService removeMember(String address) {
    context.removeMember(address);
    return this;
  }

  @Override
  public ReplicationService setMembers(Set<String> addresses) {
    context.setMembers(addresses);
    return this;
  }

  @Override
  public ReplicationService setMembers(String... addresses) {
    context.setMembers(addresses);
    return this;
  }

  @Override
  public Set<String> members() {
    return context.members();
  }

  @Override
  public ReplicationService start() {
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
  public ReplicationService start(Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
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
  public ReplicationService registerCommand(String command, Command.Type type, Function<Command, JsonObject> function) {
    stateMachine.registerCommand(command, type, function);
    return this;
  }

  @Override
  public ReplicationService unregisterCommand(String command) {
    stateMachine.unregisterCommand(command);
    return this;
  }

  @Override
  public ReplicationService submitCommand(String command, JsonObject data, Handler<AsyncResult<JsonObject>> doneHandler) {
    final Future<JsonObject> future = new DefaultFutureResult<JsonObject>().setHandler(doneHandler);
    endpoint.submit(context.currentLeader(), new SubmitRequest(new Command(command, data)), new Handler<AsyncResult<SubmitResponse>>() {
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
