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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.ClusterLocator;

/**
 * A default cluster locator implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultClusterLocator implements ClusterLocator {
  private final String address;
  private String cluster;
  private final Vertx vertx;
  private final ClusterConfig config = new ClusterConfig();
  private long timerID;
  private long interval = 10000;
  private long timeout = 5000;
  private final Map<String, Set<String>> respondents = new HashMap<>();

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      if (action != null && action.equals("join")) {
        doJoin(message);
      }
    }
  };

  public DefaultClusterLocator(Vertx vertx) {
    address = UUID.randomUUID().toString();
    this.vertx = vertx;
  }

  public DefaultClusterLocator(String address, Vertx vertx) {
    this.address = UUID.randomUUID().toString();
    cluster = address;
    this.vertx = vertx;
  }

  @Override
  public ClusterConfig config() {
    return config;
  }

  @Override
  public ClusterLocator setBroadcastAddress(String address) {
    cluster = address;
    return this;
  }

  @Override
  public String getBroadcastAddress() {
    return cluster;
  }

  @Override
  public ClusterLocator setBroadcastInterval(long interval) {
    this.interval = interval;
    if (timerID > 0) {
      vertx.cancelTimer(timerID);
      timerID = vertx.setTimer(interval, new Handler<Long>() {
        @Override
        public void handle(Long timerID) {
          doBroadcast();
        }
      });
    }
    return this;
  }

  @Override
  public long getBroadcastInterval() {
    return interval;
  }

  @Override
  public ClusterLocator setBroadcastTimeout(long timeout) {
    this.timeout = timeout;
    return this;
  }

  @Override
  public long getBroadcastTimeout() {
    return timeout;
  }

  @Override
  public ClusterLocator start() {
    return start(null);
  }

  @Override
  public ClusterLocator start(Handler<AsyncResult<ClusterConfig>> doneHandler) {
    final Future<ClusterConfig> future = new DefaultFutureResult<ClusterConfig>().setHandler(doneHandler);
    vertx.eventBus().registerHandler(cluster, messageHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          doBroadcast();
          vertx.setTimer(timeout, new Handler<Long>() {
            @Override
            public void handle(Long timer) {
              timerID = vertx.setPeriodic(interval, new Handler<Long>() {
                @Override
                public void handle(Long timerID) {
                  doBroadcast();
                }
              });
              future.setResult(config);
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public void stop() {
    vertx.eventBus().unregisterHandler(cluster, messageHandler);
    if (timerID > 0) {
      vertx.cancelTimer(timerID);
    }
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    vertx.eventBus().unregisterHandler(address, messageHandler, doneHandler);
    if (timerID > 0) {
      vertx.cancelTimer(timerID);
    }
  }

  private void doBroadcast() {
    final String id = UUID.randomUUID().toString();
    respondents.put(id, new HashSet<String>());
    vertx.eventBus().publish(cluster, new JsonObject().putString("action", "broadcast")
        .putString("address", address).putString("id", id));
    final Set<String> currentMembers = config.getMembers();
    vertx.setTimer(timeout, new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        Set<String> responded = respondents.remove(id);
        for (String address : currentMembers) {
          if (!responded.contains(address) && config.containsMember(address)) {
            config.removeMember(address);
          }
        }
      }
    });
  }

  private void doJoin(final Message<JsonObject> message) {
    final String id = message.body().getString("id");
    final String address = message.body().getString("address");
    if (id != null && address != null && respondents.containsKey(id)) {
      respondents.get(id).add(address);
      if (!config.containsMember(address)) {
        config.addMember(address);
      }
    }
  }

}
