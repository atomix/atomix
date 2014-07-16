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

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.eventbus.ReplyException;
import org.vertx.java.core.json.JsonObject;

/**
 * Event bus copycat service.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusService implements Service {
  private final String address;
  private final CopyCat copyCat;

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(final Message<JsonObject> message) {
      String action = message.body().getString("action");
      if (action != null) {
        copyCat.submitCommand(action, message.body(), new Handler<AsyncResult<JsonObject>>() {
          @Override
          public void handle(AsyncResult<JsonObject> result) {
            if (result.failed()) {
              message.fail(((ReplyException) result.cause()).failureCode(), result.cause().getMessage());
            } else {
              message.reply(new JsonObject().putString("status", "ok").putValue("result", result.result()));
            }
          }
        });
      }
    }
  };

  public EventBusService(String address, CopyCat copyCat) {
    this.address = address;
    this.copyCat = copyCat;
  }

  @Override
  public Service start() {
    return start(null);
  }

  @Override
  public Service start(Handler<AsyncResult<Void>> doneHandler) {
    copyCat.vertx.eventBus().registerHandler(address, messageHandler, doneHandler);
    return this;
  }

  @Override
  public void stop() {
    stop(null);
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    copyCat.vertx.eventBus().unregisterHandler(address, messageHandler, doneHandler);
  }

}
