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
package net.kuujo.copycat;

import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

/**
 * Client for communicating with other nodes from a specific node.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class StateClient {
  private static final long DEFAULT_REPLY_TIMEOUT = 15000;
  private final Vertx vertx;
  private String address;
  private Handler<PingRequest> pingHandler;
  private Handler<SyncRequest> syncHandler;
  private Handler<PollRequest> pollHandler;
  private Handler<SubmitRequest> submitHandler;

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      JsonObject request = message.body().getObject("request");
      if (action == null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "No action specified."));
      } else if (request == null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "Malformed request."));
      } else {
        switch (action) {
          case "ping":
            if (pingHandler != null) {
              pingHandler.handle(PingRequest.fromJson(request, message));
            }
            break;
          case "sync":
            if (syncHandler != null) {
              syncHandler.handle(SyncRequest.fromJson(request, message));
            }
            break;
          case "poll":
            if (pollHandler != null) {
              pollHandler.handle(PollRequest.fromJson(request, message));
            }
            break;
          case "submit":
            if (submitHandler != null) {
              submitHandler.handle(SubmitRequest.fromJson(request, message));
            }
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  public StateClient(String address, Vertx vertx) {
    this.address = address;
    this.vertx = vertx;
  }

  /**
   * Sends a ping request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param resultHandler An asynchronous handler to be called with the ping response.
   * @return The client.
   */
  public StateClient ping(String address, PingRequest request, Handler<AsyncResult<PingResponse>> resultHandler) {
    return ping(address, request, DEFAULT_REPLY_TIMEOUT, resultHandler);
  }

  /**
   * Sends a ping request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param resultHandler An asynchronous handler to be called with the ping response.
   * @return The client.
   */
  public StateClient ping(String address, PingRequest request, long timeout, final Handler<AsyncResult<PingResponse>> resultHandler) {
    JsonObject message = new JsonObject()
        .putString("action", "ping")
        .putObject("request", PingRequest.toJson(request));
    vertx.eventBus().sendWithTimeout(address, message, timeout, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<PingResponse>(result.cause()).setHandler(resultHandler);
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            new DefaultFutureResult<PingResponse>(PingResponse.fromJson(result.result().body().getObject("result"))).setHandler(resultHandler);
          } else {
            new DefaultFutureResult<PingResponse>(new CopyCatException(result.result().body().getString("message", ""))).setHandler(resultHandler);
          }
        }
      }
    });
    return this;
  }

  /**
   * Registers a ping request handler.
   * 
   * @param handler A ping request handler.
   * @return The client.
   */
  public StateClient pingHandler(Handler<PingRequest> handler) {
    pingHandler = handler;
    return this;
  }

  /**
   * Sends a sync request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param resultHandler An asynchronous handler to be called with the sync response.
   * @return The client.
   */
  public StateClient sync(String address, SyncRequest request, Handler<AsyncResult<SyncResponse>> resultHandler) {
    return sync(address, request, DEFAULT_REPLY_TIMEOUT, resultHandler);
  }

  /**
   * Sends a sync request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param resultHandler An asynchronous handler to be called with the sync response.
   * @return The client.
   */
  public StateClient sync(String address, SyncRequest request, long timeout, final Handler<AsyncResult<SyncResponse>> resultHandler) {
    JsonObject message = new JsonObject()
        .putString("action", "sync")
        .putObject("request", SyncRequest.toJson(request));
    vertx.eventBus().sendWithTimeout(address, message, timeout, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<SyncResponse>(result.cause()).setHandler(resultHandler);
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            new DefaultFutureResult<SyncResponse>(SyncResponse.fromJson(result.result().body().getObject("result"))).setHandler(resultHandler);
          } else {
            new DefaultFutureResult<SyncResponse>(new CopyCatException(result.result().body().getString("message", ""))).setHandler(resultHandler);
          }
        }
      }
    });
    return this;
  }

  /**
   * Registers async request handler.
   * 
   * @param handler An append entries request handler.
   * @return The client.
   */
  public StateClient syncHandler(Handler<SyncRequest> handler) {
    syncHandler = handler;
    return this;
  }

  /**
   * Sends a poll request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param resultHandler An asynchronous handler to be called with the poll response.
   * @return The client.
   */
  public StateClient poll(String address, PollRequest request, Handler<AsyncResult<PollResponse>> resultHandler) {
    return poll(address, request, DEFAULT_REPLY_TIMEOUT, resultHandler);
  }

  /**
   * Sends a poll request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param resultHandler An asynchronous handler to be called with the poll response.
   * @return The client.
   */
  public StateClient poll(String address, PollRequest request, long timeout, final Handler<AsyncResult<PollResponse>> resultHandler) {
    JsonObject message = new JsonObject()
        .putString("action", "poll")
        .putObject("request", PollRequest.toJson(request));
    vertx.eventBus().sendWithTimeout(address, message, timeout, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<PollResponse>(result.cause()).setHandler(resultHandler);
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            new DefaultFutureResult<PollResponse>(PollResponse.fromJson(result.result().body().getObject("result"))).setHandler(resultHandler);
          } else {
            new DefaultFutureResult<PollResponse>(new CopyCatException(result.result().body().getString("message"))).setHandler(resultHandler);
          }
        }
      }
    });
    return this;
  }

  /**
   * Registers a poll request handler.
   * 
   * @param handler A poll request handler.
   * @return The client.
   */
  public StateClient pollHandler(Handler<PollRequest> handler) {
    pollHandler = handler;
    return this;
  }

  /**
   * Sends a submit request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param resultHandler An asynchronous handler to be called with the submit response.
   * @return The client.
   */
  public StateClient submit(String address, SubmitRequest request, Handler<AsyncResult<SubmitResponse>> resultHandler) {
    return submit(address, request, DEFAULT_REPLY_TIMEOUT, resultHandler);
  }

  /**
   * Sends a submit request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param resultHandler An asynchronous handler to be called with the submit response.
   * @return The client.
   */
  public StateClient submit(String address, SubmitRequest request, long timeout, final Handler<AsyncResult<SubmitResponse>> resultHandler) {
    JsonObject message = new JsonObject()
        .putString("action", "submit")
        .putObject("request", SubmitRequest.toJson(request));
    vertx.eventBus().sendWithTimeout(address, message, timeout, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          new DefaultFutureResult<SubmitResponse>(result.cause()).setHandler(resultHandler);
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            new DefaultFutureResult<SubmitResponse>(SubmitResponse.fromJson(result.result().body().getObject("result"))).setHandler(resultHandler);
          } else {
            new DefaultFutureResult<SubmitResponse>(new CopyCatException(result.result().body().getString("message", ""))).setHandler(resultHandler);
          }
        }
      }
    });
    return this;
  }

  /**
   * Registers a submit request handler.
   * 
   * @param handler A submit request handler.
   * @return The client.
   */
  public StateClient submitHandler(Handler<SubmitRequest> handler) {
    submitHandler = handler;
    return this;
  }

  /**
   * Starts the client.
   * 
   * @return The client.
   */
  public StateClient start() {
    start(null);
    return this;
  }

  /**
   * Starts the client.
   * 
   * @param doneHandler An asynchronous handler to be called once the client is started.
   * @return The replica client.
   */
  public StateClient start(Handler<AsyncResult<Void>> doneHandler) {
    vertx.eventBus().registerHandler(address, messageHandler, doneHandler);
    return this;
  }

  /**
   * Stops the client.
   */
  public void stop() {
    stop(null);
  }

  /**
   * Stops the client.
   * 
   * @param doneHandler An asynchronous handler to be called once the client is stopped.
   */
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    vertx.eventBus().unregisterHandler(address, messageHandler, doneHandler);
  }

}
