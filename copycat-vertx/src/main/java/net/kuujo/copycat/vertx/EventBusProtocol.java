package net.kuujo.copycat.vertx;

import net.kuujo.copycat.CopyCatException;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.util.AsyncCallback;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * Vert.x event bus based protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusProtocol implements Protocol {
  private static final long DEFAULT_REPLY_TIMEOUT = 15000;
  private final Vertx vertx;
  private String address;
  private AsyncCallback<PingRequest> pingCallback;
  private AsyncCallback<SyncRequest> syncCallback;
  private AsyncCallback<PollRequest> pollCallback;
  private AsyncCallback<SubmitRequest> submitCallback;

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(final Message<JsonObject> message) {
      String action = message.body().getString("action");
      JsonObject request = message.body().getObject("request");
      if (action == null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "No action specified."));
      } else if (request == null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "Malformed request."));
      } else {
        switch (action) {
          case "ping":
            if (pingCallback != null) {
              pingCallback.complete(PingRequest.fromJson(request.encode().getBytes(), new AsyncCallback<PingResponse>() {
                @Override
                public void complete(PingResponse response) {
                  message.reply(new JsonObject(new String(PingResponse.toJson(response))));
                }
                @Override
                public void fail(Throwable t) {
                  message.reply();
                }
              }));
            }
            break;
          case "sync":
            if (syncCallback != null) {
              syncCallback.complete(SyncRequest.fromJson(request.encode().getBytes(), new AsyncCallback<SyncResponse>() {
                @Override
                public void complete(SyncResponse response) {
                  message.reply(new JsonObject(new String(SyncResponse.toJson(response))));
                }
                @Override
                public void fail(Throwable t) {
                  message.reply();
                }
              }));
            }
            break;
          case "poll":
            if (pollCallback != null) {
              pollCallback.complete(PollRequest.fromJson(request.encode().getBytes(), new AsyncCallback<PollResponse>() {
                @Override
                public void complete(PollResponse response) {
                  message.reply(new JsonObject(new String(PollResponse.toJson(response))));
                }
                @Override
                public void fail(Throwable t) {
                  message.reply();
                }
              }));
            }
            break;
          case "submit":
            if (submitCallback != null) {
              submitCallback.complete(SubmitRequest.fromJson(request.encode().getBytes(), new AsyncCallback<SubmitResponse>() {
                @Override
                public void complete(SubmitResponse response) {
                  message.reply(new JsonObject(new String(SubmitResponse.toJson(response))));
                }
                @Override
                public void fail(Throwable t) {
                  message.reply();
                }
              }));
            }
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  public EventBusProtocol(String address, Vertx vertx) {
    this.address = address;
    this.vertx = vertx;
  }

  @Override
  public Protocol ping(String address, PingRequest request, AsyncCallback<PingResponse> resultHandler) {
    return ping(address, request, DEFAULT_REPLY_TIMEOUT, resultHandler);
  }

  @Override
  public Protocol ping(String address, PingRequest request, long timeout, final AsyncCallback<PingResponse> callback) {
    JsonObject message = new JsonObject()
        .putString("action", "ping")
        .putObject("request", new JsonObject(new String(PingRequest.toJson(request))));
    vertx.eventBus().sendWithTimeout(address, message, timeout, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          callback.fail(result.cause());
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            callback.complete(PingResponse.fromJson(result.result().body().getObject("result").encode().getBytes()));
          } else {
            callback.fail(new CopyCatException(result.result().body().getString("message", "")));
          }
        }
      }
    });
    return this;
  }

  @Override
  public Protocol pingCallback(AsyncCallback<PingRequest> callback) {
    pingCallback = callback;
    return this;
  }

  @Override
  public Protocol sync(String address, SyncRequest request, AsyncCallback<SyncResponse> resultHandler) {
    return sync(address, request, DEFAULT_REPLY_TIMEOUT, resultHandler);
  }

  @Override
  public Protocol sync(String address, SyncRequest request, long timeout, final AsyncCallback<SyncResponse> callback) {
    JsonObject message = new JsonObject()
        .putString("action", "sync")
        .putObject("request", new JsonObject(new String(SyncRequest.toJson(request))));
    vertx.eventBus().sendWithTimeout(address, message, timeout, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          callback.fail(result.cause());
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            callback.complete(SyncResponse.fromJson(result.result().body().getObject("result").encode().getBytes()));
          } else {
            callback.fail(new CopyCatException(result.result().body().getString("message", "")));
          }
        }
      }
    });
    return this;
  }

  @Override
  public Protocol syncCallback(AsyncCallback<SyncRequest> callback) {
    syncCallback = callback;
    return this;
  }

  @Override
  public Protocol poll(String address, PollRequest request, AsyncCallback<PollResponse> resultHandler) {
    return poll(address, request, DEFAULT_REPLY_TIMEOUT, resultHandler);
  }

  @Override
  public Protocol poll(String address, PollRequest request, long timeout, final AsyncCallback<PollResponse> callback) {
    JsonObject message = new JsonObject()
        .putString("action", "poll")
        .putObject("request", new JsonObject(new String(PollRequest.toJson(request))));
    vertx.eventBus().sendWithTimeout(address, message, timeout, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          callback.fail(result.cause());
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            callback.complete(PollResponse.fromJson(result.result().body().getObject("result").encode().getBytes()));
          } else {
            callback.fail(new CopyCatException(result.result().body().getString("message")));
          }
        }
      }
    });
    return this;
  }

  @Override
  public Protocol pollCallback(AsyncCallback<PollRequest> callback) {
    pollCallback = callback;
    return this;
  }

  @Override
  public Protocol submit(String address, SubmitRequest request, AsyncCallback<SubmitResponse> resultHandler) {
    return submit(address, request, DEFAULT_REPLY_TIMEOUT, resultHandler);
  }

  @Override
  public Protocol submit(String address, SubmitRequest request, long timeout, final AsyncCallback<SubmitResponse> callback) {
    JsonObject message = new JsonObject()
        .putString("action", "submit")
        .putObject("request", new JsonObject(new String(SubmitRequest.toJson(request))));
    vertx.eventBus().sendWithTimeout(address, message, timeout, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          callback.fail(result.cause());
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            callback.complete(SubmitResponse.fromJson(result.result().body().getObject("result").encode().getBytes()));
          } else {
            callback.fail(new CopyCatException(result.result().body().getString("message", "")));
          }
        }
      }
    });
    return this;
  }

  @Override
  public Protocol submitCallback(AsyncCallback<SubmitRequest> callback) {
    submitCallback = callback;
    return this;
  }

  @Override
  public Protocol start(final AsyncCallback<Void> callback) {
    vertx.eventBus().registerHandler(address, messageHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          callback.fail(result.cause());
        } else {
          callback.complete(null);
        }
      }
    });
    return this;
  }

  @Override
  public void stop(final AsyncCallback<Void> callback) {
    vertx.eventBus().unregisterHandler(address, messageHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          callback.fail(result.cause());
        } else {
          callback.complete(null);
        }
      }
    });
  }

}
