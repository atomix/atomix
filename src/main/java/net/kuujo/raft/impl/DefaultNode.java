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
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.raft.Command;
import net.kuujo.raft.Function;
import net.kuujo.raft.Node;
import net.kuujo.raft.ReplicationService;

/**
 * A default node implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultNode implements Node {
  private final String cluster;
  private final String address;
  private final String id;
  private final Vertx vertx;
  private final ReplicationService service;
  private long heartbeatInterval = 1000;
  private double responseThreshold = 2;
  private final Map<String, NodeInfo> nodes = new HashMap<>();
  private Map<String, Data> data = new HashMap<>();

  private final class Data {
    private final Command command;
    private Object value;
    private long expire;
    private Data(Command command, Object value, long expire) {
      this.command = command;
      this.value = value;
      this.expire = expire;
    }
    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }

  private final Function<Command, JsonObject> set = new Function<Command, JsonObject>() {
    @Override
    public JsonObject call(Command command) {
      String key = command.data().getString("key");
      if (key == null) {
        return error("No key specified.");
      }

      Object value = command.data().getValue("value");
      if (value == null) {
        return error("No value specified.");
      }

      long expire = command.data().getLong("expire", 0);

      if (data.containsKey(key)) {
        data.remove(key).command.free();
      }

      if (value instanceof JsonObject) {
        Map<String, Data> map = new HashMap<>();
        for (Map.Entry<String, Object> entry : ((JsonObject) value).toMap().entrySet()) {
          map.put(entry.getKey(), new Data(null, entry.getValue(), 0));
        }
        data.put(key, new Data(command, map, expire > 0 ? System.currentTimeMillis() + expire : 0));
      }
      else if (value instanceof JsonArray) {
        Set<Data> set = new HashSet<>();
        for (Object item : (JsonArray) value) {
          set.add(new Data(null, item, 0));
        }
        data.put(key, new Data(command, set, expire));
      }
      else {
        data.put(key, new Data(command, value, expire));
      }
      return ok();
    }
  };

  private final Function<Command, JsonObject> get = new Function<Command, JsonObject>() {
    @Override
    public JsonObject call(Command command) {
      command.free();
      String key = command.data().getString("key");
      if (key == null) {
        return error("No key specified.");
      }
      if (data.containsKey(key)) {
        Data item = data.get(key);
        if (item.expire > System.currentTimeMillis()) {
          data.remove(key).command.free();
          return ok(new JsonObject().putValue("result", null));
        }
        return ok(new JsonObject().putValue("result", item.value));
      }
      return ok(new JsonObject().putValue("result", null));
    }
  };

  private final Function<Command, JsonObject> getset = new Function<Command, JsonObject>() {
    @Override
    public JsonObject call(Command command) {
      Object returnValue = null;
      String key = command.data().getString("key");
      if (key == null) {
        return error("No key specified.");
      }
  
      Object value = command.data().getValue("value");
      if (value == null) {
        return error("No value specified.");
      }
  
      long expire = command.data().getLong("expire", 0);
  
      if (data.containsKey(key)) {
        Data item = data.get(key);
        if (item.expire > System.currentTimeMillis()) {
          data.remove(key).command.free();
        }
        else {
          returnValue = item.value;
        }
        item.value = value;
        item.expire = expire > 0 ? System.currentTimeMillis() + expire : item.expire;
      }
      else {
        data.put(key, new Data(command, value, expire > 0 ? System.currentTimeMillis() + expire : 0));
      }
      return ok(new JsonObject().putValue("result", returnValue));
    }
  };

  private final Function<Command, JsonObject> incr = new Function<Command, JsonObject>() {
    @Override
    public JsonObject call(Command command) {
      String key = command.data().getString("key");
      if (key == null) {
        return error("No key specified.");
      }
  
      long expire = command.data().getLong("expire", 0);
      int by = command.data().getInteger("by", 1);
  
      if (data.containsKey(key)) {
        Data item = data.get(key);
        try {
          item.value = Integer.valueOf((int) item.value) + by;
          item.expire = expire > 0 ? System.currentTimeMillis() + expire : item.expire;
        }
        catch (Exception e) {
          return error("Invalid data type.");
        }
      }
      else {
        data.put(key, new Data(command, by, expire > 0 ? System.currentTimeMillis() + expire : 0));
      }
      return ok();
    }
  };

  private final Function<Command, JsonObject> decr = new Function<Command, JsonObject>() {
    @Override
    public JsonObject call(Command command) {
      String key = command.data().getString("key");
      if (key == null) {
        return error("No key specified.");
      }
  
      long expire = command.data().getLong("expire", 0);
      int by = command.data().getInteger("by", 1);
  
      if (data.containsKey(key)) {
        Data item = data.get(key);
        try {
          item.value = Integer.valueOf((int) item.value) - by;
          item.expire = expire > 0 ? System.currentTimeMillis() + expire : item.expire;
        }
        catch (Exception e) {
          return error("Invalid data type.");
        }
      }
      else {
        data.put(key, new Data(command, by, expire > 0 ? System.currentTimeMillis() + expire : 0));
      }
      return ok();
    }
  };

  private final Function<Command, JsonObject> mapget = new Function<Command, JsonObject>() {
    @SuppressWarnings("unchecked")
    @Override
    public JsonObject call(Command command) {
      command.free();
      String name = command.data().getString("name");
      if (name == null) {
        return error("No map name specified.");
      }
  
      String key = command.data().getString("key");
      if (key == null) {
        return error("No map key specified.");
      }
  
      Object value = command.data().getValue("value");
      if (value == null) {
        return error("No value specified.");
      }
  
      Object defaultValue = command.data().getValue("default");
  
      if (data.containsKey(name)) {
        Data item = data.get(name);
        if (item.expire > System.currentTimeMillis()) {
          data.remove(name).command.free();
          return ok(new JsonObject().putValue("result", null));
        }
        try {
          Map<String, Data> map = (Map<String, Data>) item.value;
          if (map.containsKey(key)) {
            Data subitem = map.get(key);
            if (subitem.expire > System.currentTimeMillis()) {
              map.remove(key).command.free();
            }
            else {
              return ok(new JsonObject().putValue("result", subitem.value));
            }
          }
          return ok(new JsonObject().putValue("result", defaultValue));
        }
        catch (ClassCastException e) {
          return error("Not a valid map name.");
        }
      }
      return ok(new JsonObject().putValue("result", defaultValue));
    }
  };

  private final Function<Command, JsonObject> mapset = new Function<Command, JsonObject>() {
    @SuppressWarnings("unchecked")
    @Override
    public JsonObject call(Command command) {
      String name = command.data().getString("name");
      if (name == null) {
        return error("No map name specified.");
      }
  
      String key = command.data().getString("key");
      if (key == null) {
        return error("No map key specified.");
      }
  
      Object value = command.data().getValue("value");
      if (value == null) {
        return error("No value specified.");
      }
  
      long expire = command.data().getLong("expire", 0);
  
      if (data.containsKey(name)) {
        Data item = data.get(name);
        Map<String, Data> map;
        try {
          map = (Map<String, Data>) item.value;
        }
        catch (ClassCastException e) {
          return error("Not a valid map.");
        }
  
        if (map.containsKey(key)) {
          Data olditem = map.get(key);
          olditem.command.free();
        }
        map.put(key, new Data(command, value, expire > 0 ? System.currentTimeMillis() + expire : 0));
        return ok();
      }
  
      Map<Object, Object> map = new HashMap<>();
      data.put(name, new Data(command, map, 0));
      map.put(key, value);
      return ok();
    }
  };

  private final Function<Command, JsonObject> maprem = new Function<Command, JsonObject>() {
    @SuppressWarnings("unchecked")
    @Override
    public JsonObject call(Command command) {
      String name = command.data().getString("name");
      if (name == null) {
        return error("No map name specified.");
      }
  
      String key = command.data().getString("key");
      if (key == null) {
        return error("No map key specified.");
      }
  
      if (data.containsKey(name)) {
        Data item = data.get(name);
        try {
          Map<String, Data> map = (Map<String, Data>) item.value;
          if (map.containsKey(key)) {
            Data subitem = map.remove(key);
            subitem.command.free();
            if (map.size() == 0) {
              data.remove(name).command.free();
            }
            return ok(new JsonObject().putValue("result", subitem.expire > System.currentTimeMillis() ? null : subitem.value));
          }
          return ok(new JsonObject().putValue("result", null));
        }
        catch (ClassCastException e) {
          return error("Not a valid map.");
        }
      }
      else {
        return ok(new JsonObject().putValue("result", null));
      }
    }
  };

  private final Function<Command, JsonObject> setadd = new Function<Command, JsonObject>() {
    @SuppressWarnings("unchecked")
    @Override
    public JsonObject call(Command command) {
      String name = command.data().getString("name");
      if (name == null) {
        return error("No set name specified.");
      }
  
      Object value = command.data().getValue("value");
      if (value == null) {
        return error("No value specified.");
      }
  
      long expire = command.data().getLong("expire", 0);
  
      if (data.containsKey(name)) {
        Data item = data.get(name);
        try {
          Set<Data> set = (Set<Data>) item.value;
          return ok(new JsonObject().putBoolean("result", set.add(new Data(command, value, expire > 0 ? System.currentTimeMillis() + expire : 0))));
        }
        catch (Exception e) {
          return error("Not a valid set.");
        }
      }
  
      Set<Data> set = new HashSet<Data>();
      data.put(name, new Data(command, set, 0));
      return ok(new JsonObject().putBoolean("result", set.add(new Data(command, value, expire > 0 ? System.currentTimeMillis() + expire : 0))));
    }
  };

  private final Function<Command, JsonObject> setcontains = new Function<Command, JsonObject>() {
    @SuppressWarnings("unchecked")
    @Override
    public JsonObject call(Command command) {
      command.free();
      String name = command.data().getString("name");
      if (name == null) {
        return error("No set name specified.");
      }
  
      Object value = command.data().getValue("value");
      if (value == null) {
        return error("No value specified.");
      }
  
      if (data.containsKey(name)) {
        Data item = data.get(name);
        try {
          Set<Data> set = (Set<Data>) item.value;
          long currentTime = System.currentTimeMillis();
          for (Data subitem : set) {
            if (subitem.expire <= currentTime && subitem.value.equals(value)) {
              return ok(new JsonObject().putBoolean("result", true));
            }
          }
          return ok(new JsonObject().putBoolean("result", false));
        }
        catch (ClassCastException e) {
          return error("Not a valid map.");
        }
      }
      else {
        return ok(new JsonObject().putBoolean("result", false));
      }
    }
  };

  private final Function<Command, JsonObject> setrem = new Function<Command, JsonObject>() {
    @SuppressWarnings("unchecked")
    @Override
    public JsonObject call(Command command) {
      String name = command.data().getString("name");
      if (name == null) {
        return error("No set name specified.");
      }
  
      Object value = command.data().getValue("value");
      if (value == null) {
        return error("No value specified.");
      }
  
      if (data.containsKey(name)) {
        Data item = data.get(name);
        try {
          Set<Data> set = (Set<Data>) item.value;
          long currentTime = System.currentTimeMillis();
          Data contained = null;
          for (Data subitem : set) {
            if (subitem.expire <= currentTime && subitem.value.equals(value)) {
              contained = subitem;
              break;
            }
          }
  
          if (contained != null) {
            set.remove(contained);
            contained.command.free();
            if (set.size() == 0) {
              data.remove(name).command.free();
            }
            return ok(new JsonObject().putBoolean("result", true));
          }
          return ok(new JsonObject().putBoolean("result", false));
        }
        catch (ClassCastException e) {
          return error("Not a valid map.");
        }
      }
      else {
        return ok(new JsonObject().putBoolean("result", false));
      }
    }
  };

  private final Function<Command, JsonObject> del = new Function<Command, JsonObject>() {
    @SuppressWarnings("unchecked")
    @Override
    public JsonObject call(Command command) {
      String key = command.data().getString("key");
      if (key == null) {
        return error("No key specified.");
      }
      if (data.containsKey(key)) {
        Data item = data.remove(key);
        if (item.value instanceof Map) {
          for (Data subitem : ((Map<String, Data>) item.value).values()) {
            subitem.command.free();
          }
        }
        else if (item.value instanceof Set) {
          for (Data subitem : (Set<Data>) item.value) {
            subitem.command.free();
          }
        }
        return ok(new JsonObject().putBoolean("result", true));
      }
      return ok(new JsonObject().putBoolean("result", false));
    }
  };

  private final Function<Command, JsonObject> type = new Function<Command, JsonObject>() {
    @Override
    public JsonObject call(Command command) {
      String key = command.data().getString("key");
      if (key == null) {
        return error("No key specified.");
      }
      if (data.containsKey(key)) {
        Data item = data.get(key);
        if (item.value instanceof Map) {
          return ok(new JsonObject().putString("result", "map"));
        }
        else if (item.value instanceof Set) {
          return ok(new JsonObject().putString("result", "set"));
        }
        else if (item.value instanceof Number) {
          return ok(new JsonObject().putString("result", "number"));
        }
        else if (item.value instanceof String) {
          return ok(new JsonObject().putString("result", "string"));
        }
        else {
          return ok(new JsonObject().putString("result", "value"));
        }
      }
      return ok(new JsonObject().putBoolean("result", false));
    }
  };

  private JsonObject ok() {
    return ok(new JsonObject());
  }

  private JsonObject ok(JsonObject data) {
    return data.putString("status", "ok");
  }

  private JsonObject error(String message) {
    return new JsonObject().putString("status", "error").putString("message", message);
  }

  private final Handler<Message<JsonObject>> clusterHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(final Message<JsonObject> message) {
      String action = message.body().getString("action");
      if (action == null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "No action indicated."));
      }
      else {
        switch (action) {
          case "broadcast":
            doClusterBroadcast(message);
            break;
          case "deploy":
            break;
          case "undeploy":
            break;
          case "type":
          case "set":
          case "get":
          case "getset":
          case "del":
          case "incr":
          case "decr":
          case "mapset":
          case "mapget":
          case "maprem":
          case "setadd":
          case "setcontains":
          case "setrem":
            service.submitCommand(action, message.body(), new Handler<AsyncResult<JsonObject>>() {
              @Override
              public void handle(AsyncResult<JsonObject> result) {
                if (result.failed()) {
                  message.reply(new JsonObject().putString("status", "error"));
                }
                else {
                  message.reply(result.result());
                }
              }
            });
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  private final Handler<Message<JsonObject>> nodeHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      if (action == null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "No action indicated."));
      }
      else {
        switch (action) {
          case "deploy":
            break;
          case "undeploy":
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  private final Handler<Message<JsonObject>> internalHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      if (action == null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "No action indicated."));
      }
      else {
        switch (action) {
          case "join":
            doInternalJoin(message);
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  public DefaultNode(String cluster, String address, Vertx vertx) {
    this.cluster = cluster;
    this.address = address;
    this.vertx = vertx;
    id = UUID.randomUUID().toString();
    service = new DefaultReplicationService(vertx);
    registerCommands();
  }

  private void registerCommands() {
    service.registerCommand("type", Command.Type.READ, type);
    service.registerCommand("set", Command.Type.WRITE, set);
    service.registerCommand("get", Command.Type.READ, get);
    service.registerCommand("getset", Command.Type.READ_WRITE, getset);
    service.registerCommand("del", Command.Type.WRITE, del);
    service.registerCommand("incr", Command.Type.WRITE, incr);
    service.registerCommand("decr", Command.Type.WRITE, decr);
    service.registerCommand("mapset", Command.Type.WRITE, mapset);
    service.registerCommand("mapget", Command.Type.READ, mapget);
    service.registerCommand("maprem", Command.Type.WRITE, maprem);
    service.registerCommand("setadd", Command.Type.WRITE, setadd);
    service.registerCommand("setcontains", Command.Type.READ, setcontains);
    service.registerCommand("setrem", Command.Type.WRITE, setrem);
  }

  @Override
  public String cluster() {
    return cluster;
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public Node setElectionTimeout(long timeout) {
    service.setElectionTimeout(timeout);
    return this;
  }

  @Override
  public long getElectionTimeout() {
    return service.getElectionTimeout();
  }

  @Override
  public Node setHeartbeatInterval(long interval) {
    heartbeatInterval = interval;
    return this;
  }

  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  @Override
  public Node setResponseThreshold(double threshold) {
    responseThreshold = threshold;
    return this;
  }

  @Override
  public double getResponseThreshold() {
    return responseThreshold;
  }

  @Override
  public Node start() {
    start(null);
    return this;
  }

  @Override
  public Node start(Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    vertx.eventBus().registerHandler(id, internalHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          vertx.setPeriodic(heartbeatInterval, new Handler<Long>() {
            @Override
            public void handle(Long timerID) {
              long currentTime = System.currentTimeMillis();
              for (final NodeInfo node : nodes.values()) {
                node.lastPingTime = currentTime;
                node.timer = vertx.setTimer(node.lastResponseTime > 0 ? node.lastResponseTime * 3 : 5000, new Handler<Long>() {
                  @Override
                  public void handle(Long timerID) {
                    service.removeMember(node.service);
                    node.timer = 0;
                  }
                });
              }
              vertx.eventBus().publish(cluster, new JsonObject().putString("action", "broadcast").putString("address", id));
            }
          });

          vertx.eventBus().registerHandler(cluster, clusterHandler, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                future.setFailure(result.cause());
              }
              else {
                vertx.eventBus().registerHandler(address, nodeHandler, new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    if (result.failed()) {
                      future.setFailure(result.cause());
                    }
                    else {
                      service.start(new Handler<AsyncResult<Void>>() {
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
        }
      }
    });
    return this;
  }

  private void doClusterBroadcast(final Message<JsonObject> message) {
    String address = message.body().getString("address");
    vertx.eventBus().send(address, new JsonObject().putString("action", "join").putString("id", id)
        .putString("address", address).putString("service", service.address()));
  }

  private void doInternalJoin(final Message<JsonObject> message) {
    String id = message.body().getString("id");
    String service = message.body().getString("service");
    NodeInfo node;
    if (nodes.containsKey(id)) {
      node = nodes.get(id);
    }
    else {
      node = new NodeInfo(id, service);
      nodes.put(node.id, node);
    }
    node.lastResponseTime = System.currentTimeMillis() - node.lastPingTime;
    if (node.timer > 0) {
      vertx.cancelTimer(node.timer);
    }
    if (!this.service.hasMember(node.service)) {
      this.service.addMember(node.service);
    }
  }

  @Override
  public void stop() {
    service.stop();
    vertx.eventBus().unregisterHandler(cluster, clusterHandler);
    vertx.eventBus().unregisterHandler(address, internalHandler);
    vertx.eventBus().unregisterHandler(id, internalHandler);
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    service.stop(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        vertx.eventBus().unregisterHandler(cluster, clusterHandler, new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            vertx.eventBus().unregisterHandler(address, nodeHandler, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                vertx.eventBus().unregisterHandler(id, internalHandler, new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    future.setResult((Void) null);
                  }
                });
              }
            });
          }
        });
      }
    });
  }

  private static class NodeInfo {
    private final String id;
    private final String service;
    private long lastPingTime;
    private long lastResponseTime;
    private long timer;
    private NodeInfo(String id, String service) {
      this.id = id;
      this.service = service;
    }
  }

}
