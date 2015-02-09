/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.example.eventlog;

import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.event.EventLog;
import net.kuujo.copycat.event.EventLogConfig;
import net.kuujo.copycat.event.retention.SizeBasedRetentionPolicy;
import net.kuujo.copycat.log.FileLog;
import net.kuujo.copycat.vertx.VertxEventBusProtocol;
import net.kuujo.copycat.vertx.VertxEventLoopExecutor;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Event log verticle.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventLogVerticle extends BusModBase implements Handler<Message<JsonObject>> {
  private Copycat copycat;
  private EventLog<String> eventLog;
  private Set<String> consumers = new HashSet<>();

  @Override
  public void handle(Message<JsonObject> message) {
    String action = getMandatoryString("action", message);
    if (action != null) {
      switch (action) {
        case "put":
          doPut(message);
          break;
        case "register":
          doRegister(message);
          break;
        default:
          sendError(message, "Invalid action");
          break;
      }
    }
  }

  /**
   * Handles a put message.
   */
  private void doPut(final Message<JsonObject> message) {
    String entry = getMandatoryString("message", message);
    if (entry != null) {
      eventLog.commit(entry).whenComplete((result, error) -> {
        if (error == null) {
          sendOK(message);
        } else {
          sendError(message, error.getMessage());
        }
      });
    }
  }

  /**
   * Handles a register message.
   */
  private void doRegister(final Message<JsonObject> message) {
    String address = getMandatoryString("address", message);
    if (address != null) {
      consumers.add(address);
      sendOK(message);
    }
  }

  /**
   * Consumes an event log entry.
   */
  private void consume(String entry) {
    for (String address : consumers) {
      eb.send(address, entry);
    }
  }

  /**
   * Starts the event log.
   */
  private void startEventLog(Handler<AsyncResult<Void>> doneHandler) {
    String name = getMandatoryStringConfig("name");
    String id = getMandatoryStringConfig("id");
    JsonArray members = getOptionalArrayConfig("cluster", new JsonArray().add(id));
    JsonArray replicas = getOptionalArrayConfig("replicas", new JsonArray());

    // Configure the Copycat cluster with the Vert.x event bus protocol and event bus members. With the event
    // bus protocol configured, Copycat will perform event log replication over the event bus using the event
    // bus addresses provided by the protocol URI - e.g. eventbus://foo
    // Because Copycat is a CP framework, we have to explicitly list all of the nodes in the cluster.
    ClusterConfig cluster = new ClusterConfig()
      .withProtocol(new VertxEventBusProtocol(vertx))
      .withLocalMember(String.format("eventbus://%s", id))
      .withMembers(((List<String>) members.toList()).stream().map(member -> String.format("eventbus://%s", member)).collect(Collectors.toList()));

    // Configure Copycat with the event bus cluster and Vert.x event loop executor.
    CopycatConfig config = new CopycatConfig()
      .withClusterConfig(cluster)
      .withDefaultExecutor(new VertxEventLoopExecutor(vertx));

    // Add the event log resource configuration with a Vert.x event loop executor to the Copycat configuration.
    // Configure the event log with a persistent log.
    EventLogConfig eventLogConfig = new EventLogConfig()
      .withReplicas(((List<String>) replicas.toList()).stream().map(member -> String.format("eventbus://%s", member)).collect(Collectors.toList()))
      .withRetentionPolicy(new SizeBasedRetentionPolicy(1024 * 1024 * 32))
      .withLog(new FileLog()
        .withDirectory(new File(new File(System.getProperty("java.io.tmpdir"), id), name))
        .withSegmentSize(1024 * 1024 * 16))
      .withExecutor(new VertxEventLoopExecutor(vertx));

    // Create and open a new Copycat instance. The Copycat instance controls the cluster of verticles and manages
    // resources within the cluster - in this case just a single event log.
    copycat = Copycat.create(config);

    // Once we create the Copycat instance, it needs to be opened. When the instance is opened, Copycat will begin
    // communicating with other nodes in the cluster and elect a leader that will control resources within the cluster.
    copycat.open().whenComplete((copycatResult, copycatError) -> {

      // Because we configured the Copycat instance with the VertxEventLoopExecutor Copycat executes
      // CompletableFuture callbacks on the Vert.x event loop, so we don't need to call runOnContext().
      if (copycatError != null) {
        new DefaultFutureResult<Void>(copycatError).setHandler(doneHandler);
      } else {

        // Create and open a new event log instance and create a lock proxy for submitting event log
        // commands and queries to the cluster. When the event log is opened, the event log will communicate
        // with other nodes in the Copycat instance cluster to elect a leader and begin replicating the state log.
        copycat.<String>createEventLog(name, eventLogConfig).open().whenComplete((eventLog, error) -> {
          if (error != null) {
            new DefaultFutureResult<Void>(error).setHandler(doneHandler);
          } else {
            this.eventLog = eventLog;
            eventLog.consumer(this::consume);
            new DefaultFutureResult<>((Void) null).setHandler(doneHandler);
          }
        });
      }
    });
  }

  @Override
  public void start(final Future<Void> startResult) {
    super.start();
    String address = config.getString("address");
    if (address != null) {
      vertx.eventBus().registerHandler(address, this, registerResult -> {
        if (registerResult.failed()) {
          startResult.setFailure(registerResult.cause());
        } else {
          startEventLog(result -> {
            if (startResult.failed()) {
              startResult.setFailure(result.cause());
            } else {
              startResult.setResult(null);
            }
          });
        }
      });
    } else {
      startEventLog(result -> {
        if (startResult.failed()) {
          startResult.setFailure(result.cause());
        } else {
          startResult.setResult(null);
        }
      });
    }
  }

}
