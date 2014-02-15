package net.kuujo.copycat.log.impl;

import java.util.List;

import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogException;

import org.vertx.java.core.Handler;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 * A logger verticle.
 *
 * @author Jordan Halterman
 */
public class Logger extends Verticle implements Handler<Message<JsonObject>> {
  private Log log;
  private String address;
  private String events;

  @Override
  public void start() {
    address = container.config().getString("address");
    events = container.config().getString("events");
    vertx.eventBus().registerLocalHandler(address, this);
    Log.Type logType = Log.Type.parse(container.config().getString("log", Log.Type.FILE.getName()));
    try {
      log = (Log) logType.getType().newInstance();
    }
    catch (Exception e) {
      throw new IllegalArgumentException("Error instantiating log instance.");
    }

    log.fullHandler(new VoidHandler() {
      @Override
      public void handle() {
        vertx.eventBus().publish(events, new JsonObject().putString("event", "full"));
      }
    });
    log.drainHandler(new VoidHandler() {
      @Override
      public void handle() {
        vertx.eventBus().publish(events, new JsonObject().putString("event", "drain"));
      }
    });

    log.setMaxSize(container.config().getLong("size", 10000));
    log.open(container.config().getString("filename"));
  }

  public void handle(Message<JsonObject> message) {
    String action = message.body().getString("action");
    if (action == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No action specified."));
    }
    else {
      switch (action) {
        case "firstIndex":
          doFirstIndex(message);
          break;
        case "firstEntry":
          doFirstEntry(message);
          break;
        case "appendEntry":
          doAppendEntry(message);
          break;
        case "containsEntry":
          doContainsEntry(message);
          break;
        case "getEntry":
          doGetEntry(message);
          break;
        case "getEntries":
          doGetEntries(message);
          break;
        case "removeBefore":
          doRemoveBefore(message);
          break;
        case "removeAfter":
          doRemoveAfter(message);
          break;
        case "lastIndex":
          doLastIndex(message);
          break;
        case "lastEntry":
          doLastEntry(message);
          break;
        default:
          message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
          break;
      }
    }
  }

  @Override
  public void stop() {
    if (log != null) {
      log.close();
    }
  }

  private void doFirstIndex(final Message<JsonObject> message) {
    try {
      message.reply(new JsonObject().putString("status", "ok").putNumber("result", log.firstIndex()));
    }
    catch (LogException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  private void doFirstEntry(final Message<JsonObject> message) {
    try {
      message.reply(new JsonObject().putString("status", "ok").putValue("result", log.firstEntry()));
    }
    catch (LogException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  private void doAppendEntry(final Message<JsonObject> message) {
    try {
      message.reply(new JsonObject().putString("status", "ok").putNumber("result", log.appendEntry(message.body().getValue("entry"))));
    }
    catch (LogException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  private void doContainsEntry(final Message<JsonObject> message) {
    try {
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", log.containsEntry(message.body().getLong("index"))));
    }
    catch (LogException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  private void doGetEntry(final Message<JsonObject> message) {
    try {
      Object entry = log.getEntry(message.body().getLong("index"));
      message.reply(new JsonObject().putString("status", "ok").putString("result", entry != null ? entry.toString() : null));
    }
    catch (LogException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  private void doGetEntries(final Message<JsonObject> message) {
    try {
      List<Object> entries = log.getEntries(message.body().getLong("start"), message.body().getLong("end"));
      message.reply(new JsonObject().putString("status", "ok").putArray("result", new JsonArray(entries.toArray(new Object[entries.size()]))));
    }
    catch (LogException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  private void doRemoveBefore(final Message<JsonObject> message) {
    try {
      log.removeBefore(message.body().getLong("index"));
      message.reply(new JsonObject().putString("status", "ok"));
    }
    catch (LogException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  private void doRemoveAfter(final Message<JsonObject> message) {
    try {
      log.removeAfter(message.body().getLong("index"));
      message.reply(new JsonObject().putString("status", "ok"));
    }
    catch (LogException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  private void doLastIndex(final Message<JsonObject> message) {
    try {
      message.reply(new JsonObject().putString("status", "ok").putNumber("result", log.lastIndex()));
    }
    catch (LogException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  private void doLastEntry(final Message<JsonObject> message) {
    try {
      message.reply(new JsonObject().putString("status", "ok").putValue("result", log.lastEntry()));
    }
    catch (LogException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

}
