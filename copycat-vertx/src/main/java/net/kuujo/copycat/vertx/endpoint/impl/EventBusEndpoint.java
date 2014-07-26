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
package net.kuujo.copycat.vertx.endpoint.impl;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.endpoint.Endpoint;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.uri.Optional;
import net.kuujo.copycat.uri.UriArgument;
import net.kuujo.copycat.uri.UriAuthority;
import net.kuujo.copycat.uri.UriHost;
import net.kuujo.copycat.uri.UriInject;
import net.kuujo.copycat.uri.UriPath;
import net.kuujo.copycat.uri.UriPort;
import net.kuujo.copycat.uri.UriSchemeSpecificPart;
import net.kuujo.copycat.util.AsyncCallback;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.json.JsonObject;

/**
 * Event bus endpoint implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusEndpoint implements Endpoint {
  private String address;
  private CopyCatContext context;
  private Vertx vertx;

  @UriInject
  public EventBusEndpoint(@UriArgument("vertx") Vertx vertx, @UriAuthority @UriSchemeSpecificPart String address) {
    this.vertx = vertx;
    this.address = address;
  }

  @UriInject
  public EventBusEndpoint(@UriHost String host, @Optional @UriPort int port, @UriPath String address) {
    final CountDownLatch latch = new CountDownLatch(1);
    vertx = new DefaultVertx(port >= 0 ? port : 0, host, new Handler<AsyncResult<Vertx>>() {
      @Override
      public void handle(AsyncResult<Vertx> result) {
        latch.countDown();
      }
    });
    try {
      latch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new ProtocolException(e);
    }
    this.address = address;
  }

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(final Message<JsonObject> message) {
      String command = message.body().getString("command");
      if (command != null) {
        Map<String, Object> args = message.body().toMap();
        args.remove("command");
        context.submitCommand(command, args, new AsyncCallback<Map<String, Object>>() {
          @Override
          public void complete(Map<String, Object> result) {
            message.reply(new JsonObject().putString("status", "ok").putString("leader", context.leader()).putObject("result", new JsonObject(result)));
          }
          @Override
          public void fail(Throwable t) {
            message.reply(new JsonObject().putString("status", "error").putString("leader", context.leader()).putString("message", t.getMessage()));
          }
        });
      }
    }
  };

  @Override
  public void init(CopyCatContext context) {
    this.context = context;
  }

  @Override
  public void start(final AsyncCallback<Void> callback) {
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
