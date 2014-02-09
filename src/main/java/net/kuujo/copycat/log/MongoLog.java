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
 * A mongo database persistent log.
 * 
 * @author Jordan Halterman
 */
public class MongoLog implements Log {
  private static final long DEFAULT_MAX_SIZE = 10000;
  private static final Serializer serializer = Serializer.getInstance();
  private final String address;
  private final String collection;
  private final Vertx vertx;
  private long maxSize = DEFAULT_MAX_SIZE;
  private long firstIndex = 0;
  private long lastIndex = 0;
  private long currentIndex = 1;
  private Handler<Void> fullHandler;
  private Handler<Void> drainHandler;
  private boolean full;

  public MongoLog(String address, String collection, Vertx vertx) {
    this.address = address;
    this.collection = collection;
    this.vertx = vertx;
  }

  @Override
  public void init(Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    firstIndex(new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          firstIndex = result.result();
          lastIndex(new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              if (result.failed()) {
                future.setFailure(result.cause());
              }
              else {
                lastIndex = result.result();
                currentIndex = lastIndex + 1;
              }
            }
          });
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
    final JsonObject query = new JsonObject()
        .putString("action", "save")
        .putString("collection", collection)
        .putObject(
            "document",
            new JsonObject().putString("type", "command").putNumber("index", index).putNumber("term", entry.term())
                .putObject("entry", serializer.<JsonObject>serialize(entry)));
    vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
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
  public Log containsEntry(long index, Handler<AsyncResult<Boolean>> containsHandler) {
    final Future<Boolean> future = new DefaultFutureResult<Boolean>().setHandler(containsHandler);
    final JsonObject query = new JsonObject().putString("action", "count").putString("collection", collection)
        .putObject("matcher", new JsonObject().putString("type", "command").putNumber("index", index));
    vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(result.result().body().getInteger("count") > 0);
        }
        else {
          future.setFailure(new LogException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log entry(long index, Handler<AsyncResult<Entry>> entryHandler) {
    final Future<Entry> future = new DefaultFutureResult<Entry>().setHandler(entryHandler);
    final JsonObject query = new JsonObject().putString("action", "findone").putString("collection", collection)
        .putObject("matcher", new JsonObject().putString("type", "command").putNumber("index", index));
    vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(serializer.deserialize(result.result().body().getObject("result").getObject("entry"), Entry.class));
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
    final JsonObject query = new JsonObject().putString("action", "find").putString("collection", collection)
        .putObject("matcher", new JsonObject().putString("type", "command")).putObject("sort", new JsonObject().putNumber("index", 1))
        .putNumber("limit", 1);
    vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(((JsonObject) result.result().body().getArray("result").get(0)).getLong("index"));
        }
        else {
          future.setFailure(new LogException(result.result().body().getString("message")));
        }
      }
    });
  }

  @Override
  public Log firstTerm(Handler<AsyncResult<Long>> handler) {
    final Future<Long> future = new DefaultFutureResult<Long>().setHandler(handler);
    final JsonObject query = new JsonObject().putString("action", "find").putString("collection", collection)
        .putObject("matcher", new JsonObject().putString("type", "command")).putObject("sort", new JsonObject().putNumber("index", 1))
        .putNumber("limit", 1);
    vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(((JsonObject) result.result().body().getArray("result").get(0)).getLong("term"));
        }
        else {
          future.setFailure(new LogException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log firstEntry(Handler<AsyncResult<Entry>> handler) {
    final Future<Entry> future = new DefaultFutureResult<Entry>().setHandler(handler);
    final JsonObject query = new JsonObject().putString("action", "find").putString("collection", collection)
        .putObject("matcher", new JsonObject().putString("type", "command")).putObject("sort", new JsonObject().putNumber("index", 1))
        .putNumber("limit", 1);
    vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(serializer.deserialize(((JsonObject) result.result().body().getArray("result").get(0)).getObject("entry"),
              Entry.class));
        }
        else {
          future.setFailure(new LogException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public long lastIndex() {
    return lastIndex;
  }

  public void lastIndex(Handler<AsyncResult<Long>> handler) {
    final Future<Long> future = new DefaultFutureResult<Long>().setHandler(handler);
    final JsonObject query = new JsonObject().putString("action", "find").putString("collection", collection)
        .putObject("matcher", new JsonObject().putString("type", "command")).putObject("sort", new JsonObject().putNumber("index", -1))
        .putNumber("limit", 1);
    vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(((JsonObject) result.result().body().getArray("result").get(0)).getLong("index"));
        }
        else {
          future.setFailure(new LogException(result.result().body().getString("message")));
        }
      }
    });
  }

  @Override
  public Log lastTerm(Handler<AsyncResult<Long>> handler) {
    final Future<Long> future = new DefaultFutureResult<Long>().setHandler(handler);
    final JsonObject query = new JsonObject().putString("action", "find").putString("collection", collection)
        .putObject("matcher", new JsonObject().putString("type", "command")).putObject("sort", new JsonObject().putNumber("index", -1))
        .putNumber("limit", 1);
    vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(((JsonObject) result.result().body().getArray("result").get(0)).getLong("term"));
        }
        else {
          future.setFailure(new LogException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log lastEntry(Handler<AsyncResult<Entry>> handler) {
    final Future<Entry> future = new DefaultFutureResult<Entry>().setHandler(handler);
    final JsonObject query = new JsonObject().putString("action", "find").putString("collection", collection)
        .putObject("matcher", new JsonObject().putString("type", "command")).putObject("sort", new JsonObject().putNumber("index", -1))
        .putNumber("limit", 1);
    vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult(serializer.deserialize(((JsonObject) result.result().body().getArray("result").get(0)).getObject("entry"),
              Entry.class));
        }
        else {
          future.setFailure(new LogException(result.result().body().getString("message")));
        }
      }
    });
    return this;
  }

  @Override
  public Log entries(long start, long end, Handler<AsyncResult<List<Entry>>> doneHandler) {
    final Future<List<Entry>> future = new DefaultFutureResult<List<Entry>>().setHandler(doneHandler);
    final List<Entry> entries = new ArrayList<>();
    final JsonObject query = new JsonObject()
        .putString("action", "find")
        .putString("collection", collection)
        .putObject(
            "matcher",
            new JsonObject().putString("type", "command").putObject("index", new JsonObject().putNumber("$ge", start))
                .putObject("index", new JsonObject().putNumber("$le", end)));
    vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("error")) {
          future.setFailure(new LogException(result.result().body().getString("message")));
        }
        else {
          JsonArray jsonEntries = result.result().body().getArray("result");
          for (Object jsonEntry : jsonEntries) {
            entries.add(serializer.deserialize(((JsonObject) jsonEntry).getObject("entry"), Entry.class));
          }
          if (!result.result().body().getString("status").equals("more-exist")) {
            future.setResult(entries);
          }
        }
      }
    });
    return this;
  }

  @Override
  public Log removeEntry(final long index, final Handler<AsyncResult<Entry>> doneHandler) {
    final Future<Entry> future = new DefaultFutureResult<Entry>().setHandler(doneHandler);
    return entry(index, new Handler<AsyncResult<Entry>>() {
      @Override
      public void handle(AsyncResult<Entry> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else {
          final Entry entry = result.result();
          final JsonObject query = new JsonObject().putString("action", "delete").putString("collection", collection)
              .putObject("matcher", new JsonObject().putString("type", "command").putNumber("index", index));
          vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
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
      }
    });
  }

  @Override
  public Log removeBefore(long index, Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    final JsonObject query = new JsonObject().putString("action", "delete").putString("collection", collection)
        .putObject("matcher", new JsonObject().putString("type", "command").putObject("index", new JsonObject().putNumber("$lt", index)));
    vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult((Void) null);
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
  public Log removeAfter(long index, Handler<AsyncResult<Void>> doneHandler) {
    final Future<Void> future = new DefaultFutureResult<Void>().setHandler(doneHandler);
    final JsonObject query = new JsonObject().putString("action", "delete").putString("collection", collection)
        .putObject("matcher", new JsonObject().putString("type", "command").putObject("index", new JsonObject().putNumber("$gt", index)));
    vertx.eventBus().sendWithTimeout(address, query, 15000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        }
        else if (result.result().body().getString("status").equals("ok")) {
          future.setResult((Void) null);
          checkSize();
        }
        else {
          future.setFailure(new LogException(result.result().body().getString("message")));
        }
      }
    });
    return this;
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
