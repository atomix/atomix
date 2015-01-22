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
package net.kuujo.copycat.vertx;

import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.event.EventLog;
import net.kuujo.copycat.event.EventLogConfig;
import net.kuujo.copycat.event.SizeBasedRetentionPolicy;
import net.kuujo.copycat.event.ZeroRetentionPolicy;
import net.kuujo.copycat.log.BufferedLog;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;
import org.vertx.testtools.TestVerticle;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.vertx.testtools.VertxAssert.*;

/**
 * Vert.x event bus protocol test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusProtocolTest extends TestVerticle {

  /**
   * Test event log verticle.
   */
  public static class EventLogVerticle extends Verticle implements Handler<Message<JsonObject>> {
    private Copycat copycat;
    private EventLog<String> log;
    private final Set<String> consumers = new HashSet<>();

    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      if (action != null) {
        switch (action) {
          case "send":
            doSend(message);
            break;
          case "register":
            doRegister(message);
            break;
          case "unregister":
            doUnregister(message);
            break;
        }
      }
    }

    @Override
    @SuppressWarnings("all")
    public void start(final Future<Void> startResult) {
      // Register an event bus handler at "name". This handler will receive send and register requests and commit
      // event messages to the Copycat event log.
      vertx.eventBus().registerHandler(container.config().getString("name"), this, registerResult -> {
        if (registerResult.failed()) {
          startResult.setFailure(registerResult.cause());
        } else {

          // Configure the Copycat cluster with the Vert.x event bus protocol and event bus members.
          ClusterConfig cluster = new ClusterConfig()
            .withProtocol(new VertxEventBusProtocol(vertx))
            .withMembers(((List<String>) container.config().getArray("cluster").toList())
              .stream()
              .collect(Collectors.mapping(address -> String.format("eventbus://%s", address), Collectors.toList())));

          // Configure Copycat with the event bus cluster and Vert.x event loop executor.
          CopycatConfig config = new CopycatConfig()
            .withClusterConfig(cluster)
            .withExecutor(new VertxEventLoopExecutor(vertx));

          // Add the event log resource configuration with a Vert.x event loop executor to the Copycat instance.
          config.addEventLogConfig("log", new EventLogConfig()
            .withLog(new BufferedLog()
              .withFlushInterval(1000 * 60)
              .withSegmentSize(1024 * 1024))
            .withExecutor(new VertxEventLoopExecutor(vertx))
            .withRetentionPolicy(new SizeBasedRetentionPolicy(1024 * 1024 * 32)));

          // Create and open a new Copycat instance.
          copycat = Copycat.create(String.format("eventbus://%s", container.config().getString("address")), config);
          copycat.open().whenComplete((copycatResult, copycatError) -> {
            if (copycatError != null) {
              startResult.setFailure(copycatError);
            } else {

              // Create and open a new event log instance and register an event consumer.
              copycat.<String>eventLog("log").open().whenComplete((log, error) -> {
                if (error != null) {
                  startResult.setFailure(error);
                } else {
                  this.log = log;
                  log.consumer(this::consume);
                  startResult.setResult(null);
                }
              });
            }
          });
        }
      });
    }

    @Override
    public void stop() {
      try {
        if (copycat != null)
          copycat.close().get();
      } catch (InterruptedException | ExecutionException e) {
      }
    }

    /**
     * Consumes event log messages.
     */
    private void consume(String message) {
      for (String address : consumers) {
        vertx.eventBus().send(address, message);
      }
    }

    /**
     * Puts a new message in the event log.
     */
    private void doSend(Message<JsonObject> message) {
      log.commit(message.body().getString("message")).whenComplete((index, error) -> {
        if (error != null) {
          message.reply(new JsonObject().putString("status", "error").putString("message", error.getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok").putNumber("index", index));
        }
      });
    }

    /**
     * Registers a consumer.
     */
    private void doRegister(Message<JsonObject> message) {
      String address = message.body().getString("address");
      if (address != null) {
        consumers.add(address);
        message.reply(new JsonObject().putString("status", "ok"));
      } else {
        message.reply(new JsonObject().putString("status", "error").putString("message", "No address specified"));
      }
    }

    /**
     * Unregisters a consumer.
     */
    private void doUnregister(Message<JsonObject> message) {
      String address = message.body().getString("address");
      if (address != null) {
        consumers.remove(address);
        message.reply(new JsonObject().putString("status", "ok"));
      } else {
        message.reply(new JsonObject().putString("status", "error").putString("message", "No address specified"));
      }
    }

  }

  @Test
  public void testEventBusProtocol() {
    // Deploy a cluster of five nodes.
    List<String> members = IntStream.range(1, 6).asLongStream().boxed().collect(Collectors.toList()).stream().collect(Collectors.mapping(num -> String.format("test%d", num), Collectors.toList()));
    deployCluster("log", members, deployResult -> {
      assertTrue(deployResult.succeeded());

      // Register an event bus handler to which events can be sent.
      vertx.eventBus().registerHandler("test", (Handler<Message<String>>) message -> {
        assertEquals("Hello world!", message.body());
        testComplete();
      }, registerResult -> {
        assertTrue(registerResult.succeeded());

        // Register the handler with the event log cluster by sending a "register" request.
        vertx.eventBus().send("log", new JsonObject().putString("action", "register").putString("address", "test"), (Handler<Message<JsonObject>>) registerReply -> {
          assertEquals("ok", registerReply.body().getString("status"));

          // Finally, send a message to the event log. The message should be logged, replicated, and sent to our consumer.
          vertx.eventBus().send("log", new JsonObject().putString("action", "send").putString("message", "Hello world!"), (Handler<Message<JsonObject>>) sendReply -> {
            assertEquals("ok", sendReply.body().getString("status"));
          });
        });
      });
    });
  }

  /**
   * Deploys a cluster of event log verticles.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void deployCluster(String name, List<String> members, Handler<AsyncResult<Void>> doneHandler) {
    AtomicInteger count = new AtomicInteger();
    for (String member : members) {
      JsonObject config = new JsonObject()
        .putString("name", name)
        .putString("address", member)
        .putArray("cluster", new JsonArray((List) members));
      container.deployVerticle(EventLogVerticle.class.getName(), config, result -> {
        assertTrue(result.succeeded());
        if (count.incrementAndGet() == members.size()) {
          new DefaultFutureResult<>((Void) null).setHandler(doneHandler);
        }
      });
    }
  }

}
