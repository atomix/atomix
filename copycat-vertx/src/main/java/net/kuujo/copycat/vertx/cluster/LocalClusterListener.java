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
package net.kuujo.copycat.vertx.cluster;

import java.util.HashSet;
import java.util.Set;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

/**
 * Local cluster listener implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class LocalClusterListener implements ClusterListener {
  private final String cluster;
  private final Vertx vertx;
  private final Set<String> members;
  private Handler<String> joinHandler;
  private Handler<String> leaveHandler;

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      String address = message.body().getString("address");
      if (action != null) {
        switch (action) {
          case "join":
            doJoin(address);
            break;
          case "leave":
            doLeave(address);
            break;
        }
      }
    }
  };

  public LocalClusterListener(String cluster, Vertx vertx) {
    this.cluster = cluster;
    this.vertx = vertx;
    this.members = vertx.sharedData().getSet(cluster);
    vertx.eventBus().registerLocalHandler(cluster, messageHandler);
  }

  @Override
  public void getMembers(Handler<AsyncResult<Set<String>>> resultHandler) {
    new DefaultFutureResult<Set<String>>(new HashSet<>(members)).setHandler(resultHandler);
  }

  @Override
  public void join(String address, Handler<AsyncResult<Void>> doneHandler) {
    if (members.add(address)) {
      vertx.eventBus().publish(cluster, new JsonObject().putString("action", "join").putString("address", address));
    }
    new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
  }

  @Override
  public void joinHandler(Handler<String> handler) {
    joinHandler = handler;
  }

  private void doJoin(String address) {
    if (joinHandler != null) {
      joinHandler.handle(address);
    }
  }

  @Override
  public void leave(String address, Handler<AsyncResult<Void>> doneHandler) {
    if (members.remove(address)) {
      vertx.eventBus().publish(cluster, new JsonObject().putString("action", "leave").putString("address", address));
    }
    new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
  }

  @Override
  public void leaveHandler(Handler<String> handler) {
    leaveHandler = handler;
  }

  private void doLeave(String address) {
    if (leaveHandler != null) {
      leaveHandler.handle(address);
    }
  }

}
