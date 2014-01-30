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

import net.kuujo.mimeo.Replica;
import net.kuujo.mimeo.ReplicationService;
import net.kuujo.mimeo.cluster.ClusterConfig;
import net.kuujo.mimeo.cluster.ClusterController;
import net.kuujo.mimeo.cluster.impl.DefaultClusterController;
import net.kuujo.mimeo.log.Log;
import net.kuujo.mimeo.log.MemoryLog;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

/**
 * A raft based replica.
 * 
 * @author Jordan Halterman
 */
public class RaftReplica implements Replica {
  private final String address;
  private final ClusterController cluster;
  private final ReplicationService service;

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(final Message<JsonObject> message) {
      String command = message.body().getString("command");
      service.submitCommand(command, message.body(), new Handler<AsyncResult<JsonObject>>() {
        @Override
        public void handle(AsyncResult<JsonObject> result) {
          if (result.failed()) {
            message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
          }
          else {
            if (!result.result().getFieldNames().contains("status")) {
              result.result().putString("status", "ok");
            }
            message.reply(result.result());
          }
        }
      });
    }
  };

  public RaftReplica(String address, Vertx vertx) {
    this(address, vertx, new MemoryLog());
  }

  public RaftReplica(String address, Vertx vertx, Log log) {
    this.address = address;
    this.cluster = new DefaultClusterController(vertx).setLocalAddress(address);
    this.service = new RaftReplicationService(address, vertx, log);
    cluster.messageHandler(messageHandler);
  }

  public RaftReplica(String address, String cluster, Vertx vertx) {
    this(address, cluster, vertx, new MemoryLog());
  }

  public RaftReplica(String address, String cluster, Vertx vertx, Log log) {
    this.address = address;
    this.cluster = new DefaultClusterController(address, cluster, vertx);
    this.service = new RaftReplicationService(address, vertx, log);
    this.cluster.messageHandler(messageHandler);
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public ClusterController cluster() {
    return cluster;
  }

  @Override
  public ReplicationService service() {
    return service;
  }

  @Override
  public Replica start() {
    return start(null);
  }

  @Override
  public Replica start(Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    cluster.start(new Handler<AsyncResult<ClusterConfig>>() {
      @Override
      public void handle(AsyncResult<ClusterConfig> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          service.start(result.result(), new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                future.setFailure(result.cause());
              }
              else {
                future.setResult((Void) null);
              }
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public void stop() {
    stop(null);
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    stopService(new DefaultFutureResult<Void>().setHandler(doneHandler));
  }

  private void stopService(final Future<Void> future) {
    service.stop(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          stopCluster(future, result.cause());
        }
        else {
          stopCluster(future, null);
        }
      }
    });
  }

  private void stopCluster(final Future<Void> future, final Throwable failure) {
    cluster.stop(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (failure != null) {
          future.setFailure(failure);
        }
        else if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          future.setResult((Void) null);
        }
      }
    });
  }

}
