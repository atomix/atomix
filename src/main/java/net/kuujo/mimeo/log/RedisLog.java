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
package net.kuujo.mimeo.log;

import java.util.ArrayList;
import java.util.List;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.mimeo.MimeoException;
import net.kuujo.mimeo.serializer.Serializer;

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
  private long currentIndex;

  public RedisLog(String address, String key, Vertx vertx) {
    this.address = address;
    this.key = key;
    this.vertx = vertx;
  }

  @Override
  public void init(final LogVisitor visitor, Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    JsonObject message = new JsonObject()
      .putString("command", "get")
      .putArray("args", new JsonArray().add(key));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("error")) {
          future.setFailure(new MimeoException(result.result().body().getString("message")));
        }
        else {
          Object index = result.result().body().getValue("value");
          if (index != null) {
            currentIndex = (long) index + 1;
          }
          applyNextEntry(0, currentIndex-1, visitor, future);
        }
      }
    });
  }

  private void applyNextEntry(final long index, final long ceiling, final LogVisitor visitor, final Future<Void> future) {
    JsonObject message = new JsonObject()
      .putString("command", "get")
      .putArray("args", new JsonArray().add(String.format("%s:%d", key, index)));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("error")) {
          future.setFailure(new MimeoException(result.result().body().getString("message")));
        }
        else {
          visitor.applyEntry(serializer.deserialize(new JsonObject(result.result().body().getString("value")), Entry.class));
          applyNextEntry(0, currentIndex-1, visitor, future);
        }
      }
    });
  }

  @Override
  public Log appendEntry(Entry entry, Handler<AsyncResult<Long>> doneHandler) {
    final Future<Long> future = new DefaultFutureResult<Long>().setHandler(doneHandler);
    final long index = currentIndex++;
    final JsonObject message = new JsonObject()
        .putString("command", "set").putArray("args", new JsonArray()
        .add(String.format("%s:%d", key, index)).add(serializer.serialize(entry).encode()));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(index);
        }
        else {
          future.setFailure(new MimeoException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log containsEntry(final long index, Handler<AsyncResult<Boolean>> containsHandler) {
    final Future<Boolean> future = new DefaultFutureResult<Boolean>().setHandler(containsHandler);
    final JsonObject message = new JsonObject()
        .putString("command", "exists")
        .putArray("args", new JsonArray().add(String.format("%s:%d", key, index)));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(result.result().body().getBoolean("value"));
        }
        else {
          future.setFailure(new MimeoException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log entry(long index, Handler<AsyncResult<Entry>> entryHandler) {
    final Future<Entry> future = new DefaultFutureResult<Entry>().setHandler(entryHandler);
    final JsonObject message = new JsonObject().putString("command", "get").putArray("args", new JsonArray().add(key).add(index));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          String value = result.result().body().getString("value");
          if (value != null) {
            future.setResult(serializer.deserialize(new JsonObject(value), Entry.class));
          }
          else {
            future.setFailure(new MimeoException("Invalid index."));
          }
        }
        else {
          future.setFailure(new MimeoException(result.result().body().getString("message")));
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
    new DefaultFutureResult<Long>().setHandler(handler).setResult(currentIndex-1);
    return this;
  }

  @Override
  public Log lastTerm(Handler<AsyncResult<Long>> handler) {
    final Future<Long> future = new DefaultFutureResult<Long>().setHandler(handler);
    return entry(currentIndex-1, new Handler<AsyncResult<Entry>>() {
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

  @Override
  public Log lastEntry(final Handler<AsyncResult<Entry>> handler) {
    return entry(currentIndex-1, handler);
  }

  @Override
  public Log entries(long start, long end, Handler<AsyncResult<List<Entry>>> doneHandler) {
    loadEntries(start, end, new ArrayList<Entry>(), new DefaultFutureResult<List<Entry>>().setHandler(doneHandler));
    return this;
  }

  private void loadEntries(final long index, final long end, final List<Entry> entries, final Future<List<Entry>> future) {
    if (index <= end) {
      final JsonObject message = new JsonObject()
          .putString("command", "get")
          .putArray("args", new JsonArray().add(String.format("%s:%d", key, index)));
      vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
        @Override
        public void handle(AsyncResult<Message<JsonObject>> result) {
          if (result.failed()) {
            future.setFailure(result.cause());
          }
          else if (result.result().body().getString("status").equals("ok")) {
            String value = result.result().body().getString("value");
            if (value != null) {
              entries.add(serializer.deserialize(new JsonObject(value), Entry.class));
              loadEntries(index+1, end, entries, future);
            }
            else {
              future.setFailure(new MimeoException("Invalid index."));
            }
          }
          else {
            future.setFailure(new MimeoException(result.result().body().getString("message")));
          }
        }
      });
    }
    else {
      future.setResult(entries);
    }
  }

  @Override
  public Log removeEntry(final long index, Handler<AsyncResult<Entry>> doneHandler) {
    final Future<Entry> future = new DefaultFutureResult<Entry>().setHandler(doneHandler);
    JsonObject message = new JsonObject().putString("command", "get")
        .putArray("args", new JsonArray().add(String.format("%s:%d", key, index)));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          String value = result.result().body().getString("value");
          if (value != null) {
            final Entry entry = serializer.deserialize(new JsonObject(value), Entry.class);
            JsonObject message = new JsonObject().putString("command", "del")
                .putArray("args", new JsonArray().add(String.format("%s:%d", key, index)));
            vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
              @Override
              public void handle(AsyncResult<Message<JsonObject>> result) {
                if (result.failed()) {
                  future.setFailure(result.cause());
                }
                else if (result.result().body().getString("status").equals("ok")) {
                  future.setResult(entry);
                }
                else {
                  future.setFailure(new MimeoException(result.result().body().getString("message")));
                }
              }
            });
          }
          else {
            future.setFailure(new MimeoException("Invalid index."));
          }
        }
        else {
          future.setFailure(new MimeoException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log removeBefore(long index, Handler<AsyncResult<Void>> doneHandler) {
    removeEntries(0, index, new DefaultFutureResult<Void>().setHandler(doneHandler));
    return this;
  }

  @Override
  public Log removeAfter(long index, Handler<AsyncResult<Void>> doneHandler) {
    removeEntries(index, currentIndex-1, new DefaultFutureResult<Void>().setHandler(doneHandler));
    return this;
  }

  private void removeEntries(final long index, final long end, final Future<Void> future) {
    if (index <= end) {
      final JsonObject message = new JsonObject()
          .putString("command", "del")
          .putArray("args", new JsonArray().add(String.format("%s:%d", key, index)));
      vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
        @Override
        public void handle(AsyncResult<Message<JsonObject>> result) {
          if (result.failed()) {
            future.setFailure(result.cause());
          }
          else if (result.result().body().getString("status").equals("ok")) {
            String value = result.result().body().getString("value");
            if (value != null) {
              removeEntries(index-1, end, future);
            }
            else {
              future.setFailure(new MimeoException("Invalid index."));
            }
          }
          else {
            future.setFailure(new MimeoException(result.result().body().getString("message")));
          }
        }
      });
    }
    else {
      future.setResult((Void) null);
    }
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
  public void free(Entry entry) {
    // Not yet implemented.
  }

  @Override
  public void free(Entry entry, Handler<AsyncResult<Void>> doneHandler) {
    // Not yet implemented.
  }

}
