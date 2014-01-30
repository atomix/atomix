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
package net.kuujo.raft.log.impl;

import java.util.ArrayList;
import java.util.List;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VertxException;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.raft.Command;
import net.kuujo.raft.log.Entry;
import net.kuujo.raft.log.Log;
import net.kuujo.raft.log.LogVisitor;
import net.kuujo.raft.serializer.Serializer;

/**
 * A redis log.
 *
 * @author Jordan Halterman
 */
public class RedisLog implements Log {
  private static final Serializer serializer = Serializer.getInstance();
  private final String address;
  private final String key;
  private final Vertx vertx;

  public RedisLog(String address, String key, Vertx vertx) {
    this.address = address;
    this.key = key;
    this.vertx = vertx;
  }

  @Override
  public void init(LogVisitor visitor, Handler<AsyncResult<Void>> doneHandler) {
    
  }

  @Override
  public Log appendEntry(Entry entry, Handler<AsyncResult<Long>> doneHandler) {
    final Future<Long> future = new DefaultFutureResult<Long>().setHandler(doneHandler);
    final JsonObject message = new JsonObject()
      .putString("command", "rpush")
      .putArray("args", new JsonArray().add(key).add(serializer.serialize(entry)));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          long index = result.result().body().getLong("value") - 1;
          future.setResult(index);
        }
        else {
          future.setFailure(new VertxException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log containsEntry(final long index, Handler<AsyncResult<Boolean>> containsHandler) {
    final Future<Boolean> future = new DefaultFutureResult<Boolean>().setHandler(containsHandler);
    final JsonObject message = new JsonObject()
      .putString("command", "llen")
      .putArray("args", new JsonArray().add(key));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(result.result().body().getLong("value") - 1 >= index);
        }
        else {
          future.setFailure(new VertxException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log entry(long index, Handler<AsyncResult<Entry>> entryHandler) {
    final Future<Entry> future = new DefaultFutureResult<Entry>().setHandler(entryHandler);
    final JsonObject message = new JsonObject()
      .putString("command", "lindex")
      .putArray("args", new JsonArray().add(key).add(index));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          JsonObject value = result.result().body().getObject("value");
          if (value != null) {
            future.setResult(serializer.deserialize(value, Entry.class));
          }
          else {
            future.setFailure(new VertxException("Invalid index."));
          }
        }
        else {
          future.setFailure(new VertxException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log firstIndex(Handler<AsyncResult<Long>> handler) {
    final Future<Long> future = new DefaultFutureResult<Long>().setHandler(handler);
    containsEntry(0, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          future.setResult((long) (result.result() ? 0 : -1));
        }
      }
    });
    return this;
  }

  @Override
  public Log firstTerm(Handler<AsyncResult<Long>> handler) {
    final Future<Long> future = new DefaultFutureResult<Long>().setHandler(handler);
    containsEntry(0, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          entry(0, new Handler<AsyncResult<Entry>>() {
            @Override
            public void handle(AsyncResult<Entry> result) {
              if (result.failed()) {
                future.setFailure(result.cause());
              }
              else {
                future.setResult(result.result().term());
              }
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public Log firstEntry(final Handler<AsyncResult<Entry>> handler) {
    containsEntry(0, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          new DefaultFutureResult<Entry>().setHandler(handler).setFailure(result.cause());
        }
        else {
          entry(0, handler);
        }
      }
    });
    return this;
  }

  @Override
  public Log lastIndex(Handler<AsyncResult<Long>> handler) {
    final Future<Long> future = new DefaultFutureResult<Long>().setHandler(handler);
    final JsonObject message = new JsonObject()
      .putString("command", "llen")
      .putArray("args", new JsonArray().add(key));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(result.result().body().getLong("value") - 1);
        }
        else {
          future.setFailure(new VertxException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log lastTerm(Handler<AsyncResult<Long>> handler) {
    final Future<Long> future = new DefaultFutureResult<Long>().setHandler(handler);
    lastIndex(new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          entry(result.result(), new Handler<AsyncResult<Entry>>() {
            @Override
            public void handle(AsyncResult<Entry> result) {
              if (result.failed()) {
                future.setFailure(result.cause());
              }
              else {
                future.setResult(result.result().term());
              }
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public Log lastEntry(final Handler<AsyncResult<Entry>> handler) {
    lastIndex(new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        if (result.failed()) {
          new DefaultFutureResult<Entry>().setHandler(handler).setFailure(result.cause());
        }
        else {
          entry(result.result(), handler);
        }
      }
    });
    return this;
  }

  @Override
  public Log entries(long start, long end, Handler<AsyncResult<List<Entry>>> doneHandler) {
    final Future<List<Entry>> future = new DefaultFutureResult<List<Entry>>().setHandler(doneHandler);
    final JsonObject message = new JsonObject()
      .putString("command", "lrange")
      .putArray("args", new JsonArray().add(key).add(start).add(end));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          JsonArray value = result.result().body().getArray("value");
          if (value != null) {
            List<Entry> entries = new ArrayList<>();
            for (Object item : value) {
              entries.add(serializer.deserialize((JsonObject) item, Entry.class));
            }
            future.setResult(entries);
          }
          else {
            future.setFailure(new VertxException("Invalid index."));
          }
        }
        else {
          future.setFailure(new VertxException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log removeEntry(final long index, Handler<AsyncResult<Entry>> doneHandler) {
    final Future<Entry> future = new DefaultFutureResult<Entry>().setHandler(doneHandler);
    JsonObject message = new JsonObject()
      .putString("command", "lindex")
      .putArray("args", new JsonArray().add(key).add(index));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          final JsonObject value = result.result().body().getObject("value");
          JsonObject message = new JsonObject()
            .putString("command", "lset")
            .putArray("args", new JsonArray().add(key).add(index).add(""));
          vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
              if (result.failed()) {
                future.setFailure(result.cause());
              }
              else if (result.result().body().getString("status").equals("ok")) {
                future.setResult(serializer.deserialize(value, Entry.class));
              }
              else {
                future.setFailure(new VertxException(result.result().body().getString("message")));
              }
            }
          });
        }
        else {
          future.setFailure(new VertxException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log removeBefore(long index, Handler<AsyncResult<Void>> doneHandler) {
    setEmpty(0, index, new DefaultFutureResult<Void>().setHandler(doneHandler));
    return this;
  }

  private void setEmpty(final long index, final long total, final Future<Void> future) {
    if (index <= total) {
      final JsonObject message = new JsonObject()
        .putString("command", "lset")
        .putArray("args", new JsonArray().add(key).add(index).add(""));
      vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
        @Override
        public void handle(AsyncResult<Message<JsonObject>> result) {
          if (result.failed()) {
            future.setFailure(result.cause());
          }
          else if (result.result().body().getString("status").equals("ok")) {
            setEmpty(index+1, total, future);
          }
          else {
            future.setFailure(new VertxException(result.result().body().getString("message")));
          }
        }
      });
    }
    else {
      future.setResult((Void) null);
    }
  }

  @Override
  public Log removeAfter(long index, Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    final JsonObject message = new JsonObject()
      .putString("command", "ltrim")
      .putArray("args", new JsonArray().add(key).add(0).add(index));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult((Void) null);
        }
        else {
          future.setFailure(new VertxException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log floor(Handler<AsyncResult<Long>> doneHandler) {
    // Not yet implemented.
    return this;
  }

  @Override
  public Log floor(long index, Handler<AsyncResult<Void>> doneHandler) {
    // Not yet implemented.
    return this;
  }

  @Override
  public void free(String command) {
    // Not yet implemented.
  }

  @Override
  public void free(String command, Handler<AsyncResult<Void>> doneHandler) {
    // Not yet implemented.
  }

  @Override
  public void free(Command command) {
    // Not yet implemented.
  }

  @Override
  public void free(Command command, Handler<AsyncResult<Void>> doneHandler) {
    // Not yet implemented.
  }

}
