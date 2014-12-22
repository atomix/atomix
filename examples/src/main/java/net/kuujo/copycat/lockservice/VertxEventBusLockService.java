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
package net.kuujo.copycat.lockservice;

import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.VertxExecutionContext;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.AsyncLock;
import net.kuujo.copycat.protocol.VertxEventBusProtocol;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 * This is a replicated lock service accessible over the Vert.x event bus.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusLockService extends Verticle {
  private Copycat copycat;
  private AsyncLock lock;

  private final Handler<Message<JsonObject>> messageHandler = message -> {
    String action = message.body().getString("action");
    if (action == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No action specified"));
    } else {
      switch (action) {
        case "lock":
          lock.lock().whenComplete((result, error) -> {
            if (error == null) {
              message.reply(new JsonObject().putString("status", "ok"));
            } else {
              message.reply(new JsonObject().putString("status", "error").putString("message", error.getMessage()));
            }
          });
          break;
        case "unlock":
          lock.unlock().whenComplete((result, error) -> {
            if (error == null) {
              message.reply(new JsonObject().putString("status", "ok"));
            } else {
              message.reply(new JsonObject().putString("status", "error").putString("message", error.getMessage()));
            }
          });
          break;
        default:
          message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
          break;
      }
    }
  };

  @Override
  public void start(final Future<Void> startResult) {
    String address = container.config().getString("address", "lock");

    // Create a Copycat cluster configuration using the Vert.x event bus protocol.
    ClusterConfig cluster = new ClusterConfig()
      .withProtocol(new VertxEventBusProtocol(vertx))
      .withMembers("eventbus://lock1", "eventbus://lock2", "eventbus://lock3");

    // Create a Copycat instance and give it a reference to the local Vert.x instance in order to run
    // asynchronous calls on the Vert.x event loop.
    copycat = Copycat.create("eventbus://lock1", cluster, new VertxExecutionContext(vertx));

    // Open the Copycat instance and create a lock instance.
    copycat.open().whenComplete((copycatResult, copycatError) -> {
      if (copycatError == null) {

        // Create and open the log.
        copycat.getLock(address).whenComplete((lock, error) -> {
          lock.open().whenComplete((lockResult, lockError) -> {
            if (lockError == null) {

              // Register an event bus handler for operating on the lock.
              vertx.eventBus().registerHandler(address, messageHandler, result -> {
                if (result.succeeded()) {
                  startResult.setResult(null);
                } else {
                  startResult.setFailure(result.cause());
                }
              });

            } else {
              startResult.setFailure(lockError);
            }
          });
        });
      } else {
        startResult.setFailure(copycatError);
      }
    });
  }

}
