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
package net.kuujo.copycat.impl;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.copycat.CopyCatNode;
import net.kuujo.copycat.CopyCatServiceEndpoint;

/**
 * A default service endpoint implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultCopyCatServiceEndpoint implements CopyCatServiceEndpoint {
  private String address;
  private final CopyCatNode copyCatNode;
  private final Vertx vertx;
  private boolean running;

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(final Message<JsonObject> message) {
      final String command = message.body().getString("command");
      if (command != null) {
        copyCatNode.submitCommand(command, message.body(), new Handler<AsyncResult<JsonObject>>() {
          @Override
          public void handle(AsyncResult<JsonObject> result) {
            if (result.failed()) {
              message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
            }
            else {
              message.reply(new JsonObject().putString("status", "ok").putObject("result", result.result()));
            }
          }
        });
      }
      else {
        message.reply(new JsonObject().putString("status", "error").putString("message", "No command specified."));
      }
    }
  };

  public DefaultCopyCatServiceEndpoint(CopyCatNode copyCatNode, Vertx vertx) {
    this.copyCatNode = copyCatNode;
    this.vertx = vertx;
  }

  public DefaultCopyCatServiceEndpoint(String address, CopyCatNode copyCatNode, Vertx vertx) {
    this.address = address;
    this.copyCatNode = copyCatNode;
    this.vertx = vertx;
  }

  @Override
  public CopyCatServiceEndpoint setAddress(String address) {
    if (running) throw new IllegalStateException("Cannot modify endpoint address during operation.");
    this.address = address;
    return this;
  }

  @Override
  public String getAddress() {
    return address;
  }

  @Override
  public CopyCatServiceEndpoint start() {
    running = true;
    vertx.eventBus().registerHandler(address, messageHandler);
    return this;
  }

  @Override
  public CopyCatServiceEndpoint start(Handler<AsyncResult<Void>> doneHandler) {
    running = true;
    vertx.eventBus().registerHandler(address, messageHandler, doneHandler);
    return this;
  }

  @Override
  public void stop() {
    vertx.eventBus().unregisterHandler(address, messageHandler);
    running = false;
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    vertx.eventBus().unregisterHandler(address, messageHandler, doneHandler);
    running = false;
  }

}
