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
import net.kuujo.copycat.CopyCatNode;
import net.kuujo.copycat.CopyCatService;
import net.kuujo.copycat.CopyCatServiceEndpoint;
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
  private final CopyCatNode copyCatNode;
  private final CopyCatServiceEndpoint endpoint;
  private final ClusterController cluster;

  public DefaultCopyCatService(Vertx vertx) {
    copyCatNode = new DefaultCopyCatNode(UUID.randomUUID().toString(), vertx);
    endpoint = new DefaultCopyCatServiceEndpoint(copyCatNode, vertx);
    cluster = new DefaultClusterController(vertx);
  }

  public DefaultCopyCatService(String address, Vertx vertx) {
    copyCatNode = new DefaultCopyCatNode(UUID.randomUUID().toString(), vertx);
    endpoint = new DefaultCopyCatServiceEndpoint(address, copyCatNode, vertx);
    cluster = new DefaultClusterController(copyCatNode.getAddress(), String.format("%s.cluster", address), vertx);
  }

  public DefaultCopyCatService(String address, Vertx vertx, Log log) {
    copyCatNode = new DefaultCopyCatNode(UUID.randomUUID().toString(), vertx, log);
    endpoint = new DefaultCopyCatServiceEndpoint(address, copyCatNode, vertx);
    cluster = new DefaultClusterController(copyCatNode.getAddress(), String.format("%s.cluster", address), vertx);
  }

  @Override
  public CopyCatService setLocalAddress(String address) {
    copyCatNode.setAddress(address);
    cluster.setLocalAddress(address);
    return this;
  }

  @Override
  public String getLocalAddress() {
    return copyCatNode.getAddress();
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
    copyCatNode.setElectionTimeout(timeout);
    return this;
  }

  @Override
  public long getElectionTimeout() {
    return copyCatNode.getElectionTimeout();
  }

  @Override
  public CopyCatService setHeartbeatInterval(long interval) {
    copyCatNode.setHeartbeatInterval(interval);
    cluster.setBroadcastInterval(interval);
    cluster.setBroadcastTimeout(interval / 2);
    return this;
  }

  @Override
  public long getHeartbeatInterval() {
    return copyCatNode.getHeartbeatInterval();
  }

  @Override
  public boolean isUseAdaptiveTimeouts() {
    return copyCatNode.isUseAdaptiveTimeouts();
  }

  @Override
  public CopyCatService useAdaptiveTimeouts(boolean useAdaptive) {
    copyCatNode.useAdaptiveTimeouts(useAdaptive);
    return this;
  }

  @Override
  public double getAdaptiveTimeoutThreshold() {
    return copyCatNode.getAdaptiveTimeoutThreshold();
  }

  @Override
  public CopyCatService setAdaptiveTimeoutThreshold(double threshold) {
    copyCatNode.setAdaptiveTimeoutThreshold(threshold);
    return this;
  }

  @Override
  public boolean isRequireWriteMajority() {
    return copyCatNode.isRequireWriteMajority();
  }

  @Override
  public CopyCatService setRequireWriteMajority(boolean require) {
    copyCatNode.setRequireWriteMajority(require);
    return this;
  }

  @Override
  public boolean isRequireReadMajority() {
    return copyCatNode.isRequireReadMajority();
  }

  @Override
  public CopyCatService setRequireReadMajority(boolean require) {
    copyCatNode.setRequireReadMajority(require);
    return this;
  }

  @Override
  public <I, O> CopyCatService registerCommand(String commandName, Function<Command<I>, O> function) {
    copyCatNode.registerCommand(commandName, function);
    return this;
  }

  @Override
  public <I, O> CopyCatService registerCommand(String commandName, Type type, Function<Command<I>, O> function) {
    copyCatNode.registerCommand(commandName, type, function);
    return this;
  }

  @Override
  public CopyCatService unregisterCommand(String commandName) {
    copyCatNode.unregisterCommand(commandName);
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
          copyCatNode.setClusterConfig(result.result());
          copyCatNode.start(new Handler<AsyncResult<Void>>() {
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
    copyCatNode.stop(new Handler<AsyncResult<Void>>() {
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
