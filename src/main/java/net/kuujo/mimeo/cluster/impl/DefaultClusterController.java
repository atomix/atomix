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
package net.kuujo.mimeo.cluster.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.kuujo.mimeo.cluster.ClusterController;
import net.kuujo.mimeo.cluster.config.ClusterConfig;
import net.kuujo.mimeo.cluster.config.impl.DynamicClusterConfig;
import net.kuujo.mimeo.cluster.config.impl.StaticClusterConfig;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

/**
 * A default cluster implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultClusterController implements ClusterController {
  private static final long MINIMUM_RESPONSE_TIME = 100;
  private String cluster;
  private String address;
  private final String internalAddress = UUID.randomUUID().toString();
  private MembershipType type = MembershipType.STATIC;
  private ClusterConfig config = new StaticClusterConfig();
  private final Vertx vertx;
  private Map<String, Long> nodes = new HashMap<>();
  private Map<String, Long> timers = new HashMap<>();
  private long timerID;
  private long lastBroadcastTime;
  private String lastBroadcastId;
  private Handler<Message<JsonObject>> messageHandler;
  private final Handler<Message<JsonObject>> wrappedMessageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      final String action = message.body().getString("action");
      if (action != null) {
        switch (action) {
          case "command":
            if (messageHandler != null) {
              messageHandler.handle(message);
            }
            else {
              message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action."));
            }
            break;
          case "broadcast":
            doBroadcast(message);
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action."));
            break;
        }
      }
      else if (messageHandler != null) {
        messageHandler.handle(message);
      }
    }
  };

  private final Handler<Message<JsonObject>> internalMessageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      final String action = message.body().getString("action");
      if (action != null) {
        switch (action) {
          case "join":
            doJoin(message);
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action."));
            break;
        }
      }
    }
  };

  public DefaultClusterController(Vertx vertx) {
    this.vertx = vertx;
  }

  public DefaultClusterController(String cluster, Vertx vertx) {
    this.cluster = cluster;
    this.vertx = vertx;
  }

  public DefaultClusterController(String address, String cluster, Vertx vertx) {
    this.address = address;
    config.addMember(address);
    this.cluster = cluster;
    this.vertx = vertx;
  }

  @Override
  public ClusterController setLocalAddress(String address) {
    if (this.address != null) {
      config.removeMember(this.address);
    }
    this.address = address;
    config.addMember(address);
    return this;
  }

  @Override
  public String getLocalAddress() {
    return address;
  }

  @Override
  public ClusterController setClusterAddress(String address) {
    this.cluster = address;
    return this;
  }

  @Override
  public String getClusterAddress() {
    return cluster;
  }

  @Override
  public ClusterController setMembershipType(MembershipType type) {
    this.type = type;
    ClusterConfig oldConfig = config;
    switch (type) {
      case STATIC:
        config = new StaticClusterConfig();
        break;
      case DYNAMIC:
        config = new DynamicClusterConfig();
        break;
    }
    config.setMembers(oldConfig.getMembers());
    return this;
  }

  @Override
  public MembershipType getMembershipType() {
    return type;
  }

  @Override
  public ClusterController addMember(String address) {
    if (!config.containsMember(address)) {
      config.addMember(address);
    }
    return this;
  }

  @Override
  public ClusterController removeMember(String address) {
    if (config.containsMember(address)) {
      config.removeMember(address);
    }
    return this;
  }

  @Override
  public ClusterController setMembers(String... members) {
    config.setMembers(members);
    return this;
  }

  @Override
  public ClusterController setMembers(Set<String> members) {
    config.setMembers(members);
    return this;
  }

  @Override
  public ClusterController start(Handler<AsyncResult<ClusterConfig>> doneHandler) {
    config.lock();
    final Future<ClusterConfig> future = new DefaultFutureResult<ClusterConfig>().setHandler(doneHandler);
    vertx.eventBus().registerHandler(cluster, wrappedMessageHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          vertx.eventBus().registerHandler(internalAddress, internalMessageHandler, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                vertx.eventBus().unregisterHandler(cluster, wrappedMessageHandler);
                future.setFailure(result.cause());
              }
              else {
                timerID = vertx.setPeriodic(2500, new Handler<Long>() {
                  @Override
                  public void handle(Long timerID) {
                    doBroadcast();
                  }
                });
                future.setResult(config);
              }
            }
          });
        }
      }
    });
    return this;
  }

  /**
   * Broadcasts a message to the cluster. Nodes should respond to the message to
   * indicate participation in the cluster.
   */
  private void doBroadcast() {
    lastBroadcastTime = System.currentTimeMillis();
    lastBroadcastId = UUID.randomUUID().toString();
    vertx.eventBus().publish(cluster,
        new JsonObject().putString("action", "broadcast").putString("address", internalAddress).putString("id", lastBroadcastId));
    for (Map.Entry<String, Long> entry : nodes.entrySet()) {
      final String address = entry.getKey();
      if (!timers.containsKey(address)) {
        final long lastResponseTime = entry.getValue();
        timers.put(address,
            vertx.setTimer(lastResponseTime < MINIMUM_RESPONSE_TIME ? MINIMUM_RESPONSE_TIME : lastResponseTime * 4, new Handler<Long>() {
              @Override
              public void handle(Long timerID) {
                nodes.remove(address);
                timers.remove(address);
                if (config.containsMember(address)) {
                  try {
                    config.removeMember(address);
                  }
                  catch (IllegalStateException e) {
                    // Fail silently.
                  }
                }
              }
            }));
      }
    }
  }

  /**
   * Handles receipt of a broadcast message.
   */
  private void doBroadcast(final Message<JsonObject> message) {
    final String address = message.body().getString("address");
    if (address != null) {
      vertx.eventBus().send(address,
          new JsonObject().putString("action", "join").putString("address", this.address).putString("id", message.body().getString("id")));
    }
  }

  /**
   * Handles receipt of a join message.
   */
  private void doJoin(final Message<JsonObject> message) {
    // Check that this is a join from our most recent broadcast in order to
    // prevent
    // mistaking extremely delayed messages for joins of the current broadcast.
    final String id = message.body().getString("id");
    if (id == lastBroadcastId) {
      final String address = message.body().getString("address");
      if (timers.containsKey(address)) {
        vertx.cancelTimer(timers.remove(address));
      }
      else {
        nodes.put(address, System.currentTimeMillis() - lastBroadcastTime);
        if (!config.containsMember(address)) {
          try {
            config.addMember(address);
          }
          catch (IllegalStateException e) {
            // Fail silently.
          }
        }
      }
    }
  }

  @Override
  public ClusterController messageHandler(Handler<Message<JsonObject>> handler) {
    messageHandler = handler;
    return this;
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    vertx.eventBus().registerHandler(cluster, wrappedMessageHandler, doneHandler);
    if (timerID > 0) {
      vertx.cancelTimer(timerID);
      timerID = 0;
    }
  }

}
