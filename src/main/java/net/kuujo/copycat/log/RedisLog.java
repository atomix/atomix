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
package net.kuujo.copycat.log;

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

import net.kuujo.copycat.serializer.Serializer;

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
  private long maxSize;
  private long firstIndex;
  private long lastIndex;
  private long currentIndex;
  private Handler<Void> fullHandler;
  private Handler<Void> drainHandler;
  private boolean full;

  public RedisLog(String address, String key, Vertx vertx) {
    this.address = address;
    this.key = key;
    this.vertx = vertx;
  }

  @Override
  public void init(Handler<AsyncResult<Void>> doneHandler) {
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
          future.setFailure(new LogException(result.result().body().getString("message")));
        }
        else {
          Object index = result.result().body().getValue("value");
          if (index != null) {
            firstIndex = 1;
            lastIndex = (long) index;
            currentIndex = lastIndex + 1;
          }
        }
      }
    });
  }

  @Override
  public Log setMaxSize(long maxSize) {
    this.maxSize = maxSize;
    return this;
  }

  @Override
  public long getMaxSize() {
    return maxSize;
  }

  @Override
  public Log fullHandler(Handler<Void> handler) {
    fullHandler = handler;
    return this;
  }

  @Override
  public Log drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  @Override
  public Log appendEntry(Entry entry, Handler<AsyncResult<Long>> doneHandler) {
    final Future<Long> future = new DefaultFutureResult<Long>().setHandler(doneHandler);
    final long index = currentIndex++;
    final JsonObject message = new JsonObject()
        .putString("command", "set").putArray("args", new JsonArray()
        .add(String.format("%s:%d", key, index)).add(serializer.<JsonObject>serialize(entry).encode()));
    vertx.eventBus().sendWithTimeout(address, message, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(index);
          checkSize();
        }
        else {
          future.setFailure(new LogException(result.result().body().getString("message")));
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
          future.setFailure(new LogException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log getEntry(long index, Handler<AsyncResult<Entry>> entryHandler) {
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
            future.setFailure(new LogException("Invalid index."));
          }
        }
        else {
          future.setFailure(new LogException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public long firstIndex() {
    return firstIndex;
  }

  public void firstIndex(Handler<AsyncResult<Long>> handler) {
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
          getEntry(0, new Handler<AsyncResult<Entry>>() {
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
          getEntry(0, handler);
        }
      }
    });
    return this;
  }

  @Override
  public long lastIndex() {
    return lastIndex;
  }

  @Override
  public Log lastTerm(Handler<AsyncResult<Long>> handler) {
    final Future<Long> future = new DefaultFutureResult<Long>().setHandler(handler);
    return getEntry(currentIndex-1, new Handler<AsyncResult<Entry>>() {
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
    return getEntry(currentIndex-1, handler);
  }

  @Override
  public Log getEntries(long start, long end, Handler<AsyncResult<List<Entry>>> doneHandler) {
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
              future.setFailure(new LogException("Invalid index."));
            }
          }
          else {
            future.setFailure(new LogException(result.result().body().getString("message")));
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
                  checkSize();
                }
                else {
                  future.setFailure(new LogException(result.result().body().getString("message")));
                }
              }
            });
          }
          else {
            future.setFailure(new LogException("Invalid index."));
          }
        }
        else {
          future.setFailure(new LogException(result.result().body().getString("message")));
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
              future.setFailure(new LogException("Invalid index."));
            }
          }
          else {
            future.setFailure(new LogException(result.result().body().getString("message")));
          }
        }
      });
    }
    else {
      future.setResult((Void) null);
      checkSize();
    }
  }

  /**
   * Checks the log size.
   */
  private void checkSize() {
    if (!full) {
      if (lastIndex - firstIndex >= maxSize) {
        full = true;
        if (fullHandler != null) {
          fullHandler.handle((Void) null);
        }
      }
    }
    else {
      if (lastIndex - firstIndex < maxSize) {
        full = false;
        if (drainHandler != null) {
          drainHandler.handle((Void) null);
        }
      }
    }
  }

}
