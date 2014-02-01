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
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.copycat.cluster.ClusterEndpoint;

/**
 * A default cluster endpoint implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultClusterEndpoint implements ClusterEndpoint {
  private String address;
  private String cluster;
  private final Vertx vertx;

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      final String action = message.body().getString("action");
      if (action != null && action.equals("broadcast")) {
        doBroadcast(message);
      }
    }
  };

  public DefaultClusterEndpoint(Vertx vertx) {
    this.vertx = vertx;
  }

  public DefaultClusterEndpoint(String address, String cluster, Vertx vertx) {
    this.address = address;
    this.cluster = cluster;
    this.vertx = vertx;
  }

  @Override
  public ClusterEndpoint setLocalAddress(String address) {
    this.address = address;
    return this;
  }

  @Override
  public String getLocalAddress() {
    return address;
  }

  @Override
  public ClusterEndpoint setBroadcastAddress(String address) {
    cluster = address;
    return this;
  }

  @Override
  public String getBroadcastAddress() {
    return cluster;
  }

  @Override
  public ClusterEndpoint start() {
    vertx.eventBus().registerHandler(cluster, messageHandler);
    return this;
  }

  @Override
  public ClusterEndpoint start(Handler<AsyncResult<Void>> doneHandler) {
    vertx.eventBus().registerHandler(cluster, messageHandler, doneHandler);
    return this;
  }

  @Override
  public void stop() {
    vertx.eventBus().unregisterHandler(cluster, messageHandler);
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    vertx.eventBus().unregisterHandler(cluster, messageHandler, doneHandler);
  }

  private void doBroadcast(final Message<JsonObject> message) {
    final String id = message.body().getString("id");
    final String address = message.body().getString("address");
    vertx.eventBus().send(address, new JsonObject().putString("action", "join")
        .putString("address", this.address).putString("id", id));
  }

}
