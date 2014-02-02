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

import java.util.UUID;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.Function;
import net.kuujo.copycat.Replica;
import net.kuujo.copycat.CopyCatService;
import net.kuujo.copycat.ReplicaEndpoint;
import net.kuujo.copycat.Command.Type;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.ClusterController;
import net.kuujo.copycat.cluster.impl.DefaultClusterController;
import net.kuujo.copycat.log.Log;

/**
 * A default service implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultCopyCatService implements CopyCatService {
  private final Replica replica;
  private final ReplicaEndpoint endpoint;
  private final ClusterController cluster;

  public DefaultCopyCatService(Vertx vertx) {
    replica = new DefaultReplica(UUID.randomUUID().toString(), vertx);
    endpoint = new DefaultReplicaEndpoint(replica, vertx);
    cluster = new DefaultClusterController(vertx);
  }

  public DefaultCopyCatService(String address, Vertx vertx) {
    replica = new DefaultReplica(UUID.randomUUID().toString(), vertx);
    endpoint = new DefaultReplicaEndpoint(address, replica, vertx);
    cluster = new DefaultClusterController(replica.getAddress(), String.format("%s.cluster", address), vertx);
  }

  public DefaultCopyCatService(String address, Vertx vertx, Log log) {
    replica = new DefaultReplica(UUID.randomUUID().toString(), vertx, log);
    endpoint = new DefaultReplicaEndpoint(address, replica, vertx);
    cluster = new DefaultClusterController(replica.getAddress(), String.format("%s.cluster", address), vertx);
  }

  public DefaultCopyCatService(String address, Vertx vertx, Replica replica) {
    this.replica = replica;
    endpoint = new DefaultReplicaEndpoint(address, replica, vertx);
    cluster = new DefaultClusterController(replica.getAddress(), String.format("%s.cluster", address), vertx);
  }

  public DefaultCopyCatService(String address, Vertx vertx, Replica replica, ClusterController cluster) {
    this.replica = replica;
    endpoint = new DefaultReplicaEndpoint(address, replica, vertx);
    this.cluster = cluster;
  }

  @Override
  public Replica replica() {
    return replica;
  }

  @Override
  public ReplicaEndpoint endpoint() {
    return endpoint;
  }

  @Override
  public CopyCatService setLocalAddress(String address) {
    replica.setAddress(address);
    cluster.setLocalAddress(address);
    return this;
  }

  @Override
  public String getLocalAddress() {
    return replica.getAddress();
  }

  @Override
  public CopyCatService setServiceAddress(String address) {
    endpoint.setAddress(address);
    return this;
  }

  @Override
  public String getServiceAddress() {
    return endpoint.getAddress();
  }

  @Override
  public CopyCatService setBroadcastAddress(String address) {
    cluster.setBroadcastAddress(address);
    return this;
  }

  @Override
  public String getBroadcastAddress() {
    return cluster.getBroadcastAddress();
  }

  @Override
  public CopyCatService setElectionTimeout(long timeout) {
    replica.setElectionTimeout(timeout);
    return this;
  }

  @Override
  public long getElectionTimeout() {
    return replica.getElectionTimeout();
  }

  @Override
  public CopyCatService setHeartbeatInterval(long interval) {
    replica.setHeartbeatInterval(interval);
    cluster.setBroadcastInterval(interval);
    cluster.setBroadcastTimeout(interval / 2);
    return this;
  }

  @Override
  public long getHeartbeatInterval() {
    return replica.getHeartbeatInterval();
  }

  @Override
  public boolean isUseAdaptiveTimeouts() {
    return replica.isUseAdaptiveTimeouts();
  }

  @Override
  public CopyCatService useAdaptiveTimeouts(boolean useAdaptive) {
    replica.useAdaptiveTimeouts(useAdaptive);
    return this;
  }

  @Override
  public double getAdaptiveTimeoutThreshold() {
    return replica.getAdaptiveTimeoutThreshold();
  }

  @Override
  public CopyCatService setAdaptiveTimeoutThreshold(double threshold) {
    replica.setAdaptiveTimeoutThreshold(threshold);
    return this;
  }

  @Override
  public boolean isRequireWriteMajority() {
    return replica.isRequireWriteMajority();
  }

  @Override
  public CopyCatService setRequireWriteMajority(boolean require) {
    replica.setRequireWriteMajority(require);
    return this;
  }

  @Override
  public boolean isRequireReadMajority() {
    return replica.isRequireReadMajority();
  }

  @Override
  public CopyCatService setRequireReadMajority(boolean require) {
    replica.setRequireReadMajority(require);
    return this;
  }

  @Override
  public <R> CopyCatService registerCommand(String commandName, Function<Command, R> function) {
    replica.registerCommand(commandName, function);
    return this;
  }

  @Override
  public <R> CopyCatService registerCommand(String commandName, Type type, Function<Command, R> function) {
    replica.registerCommand(commandName, type, function);
    return this;
  }

  @Override
  public CopyCatService unregisterCommand(String commandName) {
    replica.unregisterCommand(commandName);
    return this;
  }

  @Override
  public CopyCatService start() {
    return start(null);
  }

  @Override
  public CopyCatService start(Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    cluster.start(new Handler<AsyncResult<ClusterConfig>>() {
      @Override
      public void handle(AsyncResult<ClusterConfig> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          replica.setClusterConfig(result.result());
          replica.start(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                future.setFailure(result.cause());
              }
              else {
                endpoint.start(new Handler<AsyncResult<Void>>() {
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
    stopEndpoint(null, new DefaultFutureResult<Void>().setHandler(doneHandler));
  }

  private void stopEndpoint(final Throwable t, final Future<Void> future) {
    endpoint.stop(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        stopReplica(result.failed() ? result.cause() : null, future);
      }
    });
  }

  private void stopReplica(final Throwable t, final Future<Void> future) {
    replica.stop(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        stopCluster(t != null ? t : (result.failed() ? result.cause() : null), future);
      }
    });
  }

  private void stopCluster(final Throwable t, final Future<Void> future) {
    cluster.stop(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (t != null) {
          future.setFailure(t);
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
