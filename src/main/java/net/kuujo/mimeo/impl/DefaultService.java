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

import java.util.UUID;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.mimeo.Command;
import net.kuujo.mimeo.Command.Type;
import net.kuujo.mimeo.Function;
import net.kuujo.mimeo.Node;
import net.kuujo.mimeo.Service;
import net.kuujo.mimeo.ServiceEndpoint;
import net.kuujo.mimeo.cluster.ClusterController;
import net.kuujo.mimeo.cluster.ClusterConfig;
import net.kuujo.mimeo.cluster.impl.DefaultClusterController;
import net.kuujo.mimeo.log.Log;

/**
 * A default service implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultService implements Service {
  private final Node node;
  private final ServiceEndpoint endpoint;
  private final ClusterController cluster;

  public DefaultService(Vertx vertx) {
    node = new DefaultNode(UUID.randomUUID().toString(), vertx);
    endpoint = new DefaultServiceEndpoint(node, vertx);
    cluster = new DefaultClusterController(vertx);
  }

  public DefaultService(String address, Vertx vertx) {
    node = new DefaultNode(UUID.randomUUID().toString(), vertx);
    endpoint = new DefaultServiceEndpoint(address, node, vertx);
    cluster = new DefaultClusterController(node.getAddress(), String.format("%s.cluster", address), vertx);
  }

  public DefaultService(String address, Vertx vertx, Log log) {
    node = new DefaultNode(UUID.randomUUID().toString(), vertx, log);
    endpoint = new DefaultServiceEndpoint(address, node, vertx);
    cluster = new DefaultClusterController(node.getAddress(), String.format("%s.cluster", address), vertx);
  }

  @Override
  public Service setLocalAddress(String address) {
    node.setAddress(address);
    cluster.setLocalAddress(address);
    return this;
  }

  @Override
  public String getLocalAddress() {
    return node.getAddress();
  }

  @Override
  public Service setServiceAddress(String address) {
    endpoint.setAddress(address);
    return this;
  }

  @Override
  public String getServiceAddress() {
    return endpoint.getAddress();
  }

  @Override
  public Service setBroadcastAddress(String address) {
    cluster.setBroadcastAddress(address);
    return this;
  }

  @Override
  public String getBroadcastAddress() {
    return cluster.getBroadcastAddress();
  }

  @Override
  public Service setElectionTimeout(long timeout) {
    node.setElectionTimeout(timeout);
    return this;
  }

  @Override
  public long getElectionTimeout() {
    return node.getElectionTimeout();
  }

  @Override
  public Service setHeartbeatInterval(long interval) {
    node.setHeartbeatInterval(interval);
    cluster.setBroadcastInterval(interval);
    cluster.setBroadcastTimeout(interval / 2);
    return this;
  }

  @Override
  public long getHeartbeatInterval() {
    return node.getHeartbeatInterval();
  }

  @Override
  public boolean isUseAdaptiveTimeouts() {
    return node.isUseAdaptiveTimeouts();
  }

  @Override
  public Service useAdaptiveTimeouts(boolean useAdaptive) {
    node.useAdaptiveTimeouts(useAdaptive);
    return this;
  }

  @Override
  public double getAdaptiveTimeoutThreshold() {
    return node.getAdaptiveTimeoutThreshold();
  }

  @Override
  public Service setAdaptiveTimeoutThreshold(double threshold) {
    node.setAdaptiveTimeoutThreshold(threshold);
    return this;
  }

  @Override
  public boolean isRequireWriteMajority() {
    return node.isRequireWriteMajority();
  }

  @Override
  public Service setRequireWriteMajority(boolean require) {
    node.setRequireWriteMajority(require);
    return this;
  }

  @Override
  public boolean isRequireReadMajority() {
    return node.isRequireReadMajority();
  }

  @Override
  public Service setRequireReadMajority(boolean require) {
    node.setRequireReadMajority(require);
    return this;
  }

  @Override
  public Service registerCommand(String commandName, Function<Command, JsonObject> function) {
    node.registerCommand(commandName, function);
    return this;
  }

  @Override
  public Service registerCommand(String commandName, Type type, Function<Command, JsonObject> function) {
    node.registerCommand(commandName, type, function);
    return this;
  }

  @Override
  public Service unregisterCommand(String commandName) {
    node.unregisterCommand(commandName);
    return this;
  }

  @Override
  public Service start() {
    return start(null);
  }

  @Override
  public Service start(Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    cluster.start(new Handler<AsyncResult<ClusterConfig>>() {
      @Override
      public void handle(AsyncResult<ClusterConfig> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          node.setClusterConfig(result.result());
          node.start(new Handler<AsyncResult<Void>>() {
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
    node.stop(new Handler<AsyncResult<Void>>() {
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
