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
package net.kuujo.copycat.cluster.impl;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.ClusterController;
import net.kuujo.copycat.cluster.ClusterEndpoint;
import net.kuujo.copycat.cluster.ClusterLocator;

/**
 * A default cluster controller implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultClusterController implements ClusterController {
  private final ClusterLocator locator;
  private final ClusterEndpoint broadcaster;

  public DefaultClusterController(Vertx vertx) {
    locator = new DefaultClusterLocator(vertx);
    broadcaster = new DefaultClusterEndpoint(vertx);
  }

  public DefaultClusterController(String local, String broadcast, Vertx vertx) {
    this(vertx);
    locator.setBroadcastAddress(broadcast);
    broadcaster.setLocalAddress(local);
    broadcaster.setBroadcastAddress(broadcast);
  }

  @Override
  public ClusterConfig config() {
    return locator.config();
  }

  @Override
  public ClusterController setLocalAddress(String address) {
    broadcaster.setLocalAddress(address);
    return this;
  }

  @Override
  public String getLocalAddress() {
    return broadcaster.getLocalAddress();
  }

  @Override
  public ClusterController setBroadcastAddress(String address) {
    broadcaster.setBroadcastAddress(address);
    locator.setBroadcastAddress(address);
    return this;
  }

  @Override
  public String getBroadcastAddress() {
    return locator.getBroadcastAddress();
  }

  @Override
  public ClusterController setBroadcastInterval(long interval) {
    locator.setBroadcastInterval(interval);
    return this;
  }

  @Override
  public long getBroadcastInterval() {
    return locator.getBroadcastInterval();
  }

  @Override
  public ClusterController setBroadcastTimeout(long timeout) {
    locator.setBroadcastTimeout(timeout);
    return this;
  }

  @Override
  public long getBroadcastTimeout() {
    return locator.getBroadcastTimeout();
  }

  @Override
  public ClusterController start() {
    start(null);
    return this;
  }

  @Override
  public ClusterController start(final Handler<AsyncResult<ClusterConfig>> doneHandler) {
    broadcaster.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          new DefaultFutureResult<ClusterConfig>().setHandler(doneHandler).setFailure(result.cause());
        }
        else {
          locator.start(doneHandler);
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
  public void stop(final Handler<AsyncResult<Void>> doneHandler) {
    locator.stop(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          broadcaster.stop();
          new DefaultFutureResult<Void>().setHandler(doneHandler).setFailure(result.cause());
        }
        else {
          broadcaster.stop(doneHandler);
        }
      }
    });
  }

}
