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
package net.kuujo.copycat.log.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogException;
import net.kuujo.copycat.serializer.Serializer;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

/**
 * A proxy to a log.
 *
 * @author Jordan Halterman
 */
public final class LogProxy {
  private static final Serializer serializer  = Serializer.getInstance();
  private Log.Type type = Log.Type.FILE;
  private String filename;
  private final Vertx vertx;
  private final Container container;
  private final String events = UUID.randomUUID().toString();
  private final String address = UUID.randomUUID().toString();
  private Handler<Void> fullHandler;
  private Handler<Void> drainHandler;
  private long maxSize = 32 * 1024 * 1024;
  private String deploymentID;

  public LogProxy(String filename, Vertx vertx, Container container) {
    this.filename = filename;
    this.vertx = vertx;
    this.container = container;
  }

  private final Handler<Message<JsonObject>> eventsHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String event = message.body().getString("event");
      switch (event) {
        case "full":
          if (fullHandler != null) {
            fullHandler.handle((Void) null);
          }
          break;
        case "drain":
          if (drainHandler != null) {
            drainHandler.handle((Void) null);
          }
          break;
      }
    }
  };

  /**
   * Sets the log type.
   *
   * @param type The log type.
   * @return The log proxy.
   */
  public LogProxy setLogType(Log.Type type) {
    this.type = type;
    return this;
  }

  /**
   * Returns the log type.
   *
   * @return The log type.
   */
  public Log.Type getLogType() {
    return type;
  }

  /**
   * Sets the log file name.
   *
   * @param filename The log file name.
   * @return The log proxy.
   */
  public LogProxy setLogFile(String filename) {
    this.filename = filename;
    return this;
  }

  /**
   * Returns the log file name.
   *
   * @return The log file name.
   */
  public String getLogFile() {
    return filename;
  }

  /**
   * Sets the maximum log size.
   *
   * @param maxSize The maximum log size.
   * @return The log proxy.
   */
  public LogProxy setMaxSize(long maxSize) {
    this.maxSize = maxSize;
    return this;
  }

  /**
   * Returns the maximum log size.
   *
   * @return The maximum log size.
   */
  public long getMaxSize() {
    return maxSize;
  }

  /**
   * Opens the log.
   *
   * @param doneHandler An asynchronous handler to be called once the
   *                    log has been opened.
   */
  public void open(final Handler<AsyncResult<Void>> doneHandler) {
    container.deployWorkerVerticle(Logger.class.getName(), new JsonObject()
        .putString("address", address)
        .putString("events", events)
        .putString("filename", filename)
        .putNumber("size", maxSize)
        .putString("log", type.toString()), 1, false, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        }
        else {
          deploymentID = result.result();
          vertx.eventBus().registerLocalHandler(events, eventsHandler);
          new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
        }
      }
    });
  }

  /**
   * Closes the log.
   *
   * @param doneHandler An asynchronous handler to be called once the
   *                    log has closed.
   */
  public void close(final Handler<AsyncResult<Void>> doneHandler) {
    if (deploymentID != null) {
      container.undeployVerticle(deploymentID, doneHandler);
      deploymentID = null;
    }
  }

  /**
   * Sets a handler to be calle when the log is full.
   *
   * @param handler A handler to be called when the log is full.
   * @return The log proxy.
   */
  public LogProxy fullHandler(Handler<Void> handler) {
    fullHandler = handler;
    return this;
  }

  /**
   * Sets a handler to be calle when the log is drained.
   *
   * @param handler A handler to be called when the log is drained.
   * @return The log proxy.
   */
  public LogProxy drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  /**
   * Appends an entry to the log.
   * 
   * @param entry The entry to append.
   * @param doneHandler A handler to be called once the entry has been appended.
   * @return The log instance.
   */
  public LogProxy appendEntry(Entry entry, final Handler<AsyncResult<Long>> doneHandler) {
    vertx.eventBus().send(address, new JsonObject().putString("action", "appendEntry").putString("entry", serializer.writeString(entry)), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        if (message.body().getString("status").equals("ok")) {
          new DefaultFutureResult<Long>(message.body().getLong("result")).setHandler(doneHandler);
        }
        else {
          new DefaultFutureResult<Long>(new LogException(message.body().getString("message")));
        }
      }
    });
    return this;
  }

  /**
   * Returns a boolean indicating whether the log has an entry at the given
   * index.
   * 
   * @param index The index to check.
   * @param containsHandler A handler to be called with the contains result.
   * @return Indicates whether the log has an entry at the given index.
   */
  public LogProxy containsEntry(long index, final Handler<AsyncResult<Boolean>> containsHandler) {
    vertx.eventBus().send(address, new JsonObject().putString("action", "containsEntry").putNumber("index", index), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        if (message.body().getString("status").equals("ok")) {
          new DefaultFutureResult<Boolean>(message.body().getBoolean("result")).setHandler(containsHandler);
        }
        else {
          new DefaultFutureResult<Boolean>(new LogException(message.body().getString("message")));
        }
      }
    });
    return this;
  }

  /**
   * Returns the entry at the given index.
   * 
   * @param index The index from which to get the entry.
   * @param entryHandler A handler to be called with the entry.
   * @return The log instance.
   */
  public LogProxy getEntry(long index, final Handler<AsyncResult<Entry>> entryHandler) {
    vertx.eventBus().send(address, new JsonObject().putString("action", "getEntry").putNumber("index", index), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        if (message.body().getString("status").equals("ok")) {
          String result = message.body().getString("result");
          Entry entry = null;
          if (result != null) {
            entry = serializer.readString(result, Entry.class);
          }
          new DefaultFutureResult<Entry>(entry).setHandler(entryHandler);
        }
        else {
          new DefaultFutureResult<Boolean>(new LogException(message.body().getString("message")));
        }
      }
    });
    return this;
  }

  /**
   * Returns the first log index.
   *
   * @return
   *   The first log index.
   */
  public LogProxy firstIndex(final Handler<AsyncResult<Long>> resultHandler) {
    vertx.eventBus().send(address, new JsonObject().putString("action", "firstIndex"), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        if (message.body().getString("status").equals("ok")) {
          new DefaultFutureResult<Long>(message.body().getLong("result")).setHandler(resultHandler);
        }
        else {
          new DefaultFutureResult<Boolean>(new LogException(message.body().getString("message")));
        }
      }
    });
    return this;
  }

  /**
   * Returns the first log entry term.
   *
   * @param doneHandler A handler to be called with the term.
   * @return The log instance.
   */
  public LogProxy firstTerm(final Handler<AsyncResult<Long>> doneHandler) {
    vertx.eventBus().send(address, new JsonObject().putString("action", "firstTerm"), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        if (message.body().getString("status").equals("ok")) {
          new DefaultFutureResult<Long>(message.body().getLong("result")).setHandler(doneHandler);
        }
        else {
          new DefaultFutureResult<Boolean>(new LogException(message.body().getString("message")));
        }
      }
    });
    return this;
  }

  /**
   * Returns the first log entry.
   *
   * @param doneHandler A handler to be called with the entry.
   * @return The log instance.
   */
  public LogProxy firstEntry(final Handler<AsyncResult<Entry>> doneHandler) {
    vertx.eventBus().send(address, new JsonObject().putString("action", "firstEntry"), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        if (message.body().getString("status").equals("ok")) {
          String result = message.body().getString("result");
          Entry entry = null;
          if (result != null) {
            entry = serializer.readString(result, Entry.class);
          }
          new DefaultFutureResult<Entry>(entry).setHandler(doneHandler);
        }
        else {
          new DefaultFutureResult<Boolean>(new LogException(message.body().getString("message")));
        }
      }
    });
    return this;
  }

  /**
   * Returns the last log index.
   *
   * @return
   *   The last log index.
   */
  public LogProxy lastIndex(final Handler<AsyncResult<Long>> resultHandler) {
    vertx.eventBus().send(address, new JsonObject().putString("action", "lastIndex"), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        if (message.body().getString("status").equals("ok")) {
          new DefaultFutureResult<Long>(message.body().getLong("result")).setHandler(resultHandler);
        }
        else {
          new DefaultFutureResult<Boolean>(new LogException(message.body().getString("message")));
        }
      }
    });
    return this;
  }

  /**
   * Returns the last log entry term.
   *
   * @param doneHandler A handler to be called with the term.
   * @return The log instance.
   */
  public LogProxy lastTerm(final Handler<AsyncResult<Long>> doneHandler) {
    vertx.eventBus().send(address, new JsonObject().putString("action", "lastTerm"), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        if (message.body().getString("status").equals("ok")) {
          new DefaultFutureResult<Long>(message.body().getLong("result")).setHandler(doneHandler);
        }
        else {
          new DefaultFutureResult<Boolean>(new LogException(message.body().getString("message")));
        }
      }
    });
    return this;
  }

  /**
   * Returns the last log entry.
   *
   * @param doneHandler A handler to be called with the entry.
   * @return The log instance.
   */
  public LogProxy lastEntry(final Handler<AsyncResult<Entry>> doneHandler) {
    vertx.eventBus().send(address, new JsonObject().putString("action", "lastEntry"), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        if (message.body().getString("status").equals("ok")) {
          String result = message.body().getString("result");
          Entry entry = null;
          if (result != null) {
            entry = serializer.readString(result, Entry.class);
          }
          new DefaultFutureResult<Entry>(entry).setHandler(doneHandler);
        }
        else {
          new DefaultFutureResult<Boolean>(new LogException(message.body().getString("message")));
        }
      }
    });
    return this;
  }

  /**
   * Returns a list of log entries between two given indexes.
   * 
   * @param start The starting index.
   * @param end The ending index.
   * @return A list of entries between the two given indexes.
   */
  public LogProxy getEntries(long start, long end, final Handler<AsyncResult<List<Entry>>> doneHandler) {
    vertx.eventBus().send(address, new JsonObject().putString("action", "getEntries").putNumber("start", start).putNumber("end", end), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        if (message.body().getString("status").equals("ok")) {
          List<Entry> entries = new ArrayList<>();
          JsonArray jsonEntries = message.body().getArray("result");
          if (jsonEntries != null) {
            for (Object jsonEntry : jsonEntries) {
              entries.add(serializer.readString((String) jsonEntry, Entry.class));
            }
          }
          new DefaultFutureResult<List<Entry>>(entries).setHandler(doneHandler);
        }
        else {
          new DefaultFutureResult<Boolean>(new LogException(message.body().getString("message")));
        }
      }
    });
    return this;
  }

  /**
   * Removes all entries before the given index.
   * 
   * @param index The index before which to remove entries.
   * @return The log instance.
   */
  public LogProxy removeBefore(long index, final Handler<AsyncResult<Void>> doneHandler) {
    vertx.eventBus().send(address, new JsonObject().putString("action", "removeBefore").putNumber("index", index), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        if (message.body().getString("status").equals("ok")) {
          new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
        }
        else {
          new DefaultFutureResult<Boolean>(new LogException(message.body().getString("message")));
        }
      }
    });
    return this;
  }

  /**
   * Removes all entries after the given index.
   * 
   * @param index The index after which to remove entries.
   * @return The log instance.
   */
  public LogProxy removeAfter(long index, final Handler<AsyncResult<Void>> doneHandler) {
    vertx.eventBus().send(address, new JsonObject().putString("action", "removeAfter").putNumber("index", index), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        if (message.body().getString("status").equals("ok")) {
          new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
        }
        else {
          new DefaultFutureResult<Boolean>(new LogException(message.body().getString("message")));
        }
      }
    });
    return this;
  }

}
