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
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.log.FileLog;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.state.StateMachineConfig;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * A simple Vert.x lock service verticle.
 *
 * Configuration:
 * - address: The address of the lock service. This is used as the event bus address through which the service can be accessed.
 * - id: The unique identifier of this specific node in the lock service.
 * - cluster: An array of nodes in the lock service cluster, including this node.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LockServiceVerticle extends Verticle implements Handler<Message<JsonObject>> {
  private Copycat copycat;
  private StateMachine<LockState> stateMachine;
  private LockStateProxy lockProxy;

  @Override
  public void start(final Future<Void> startResult) {
    String id = container.config().getString("id");
    String address = container.config().getString("address");
    JsonArray members = container.config().getArray("cluster");

    // Register an event bus handler at "name". This handler will receive lock and unlock messages for the lock.
    vertx.eventBus().registerHandler(address, this, registerResult -> {
      if (registerResult.failed()) {
        startResult.setFailure(registerResult.cause());
      } else {

        // Configure the Copycat cluster with the Vert.x event bus protocol and event bus members. With the event
        // bus protocol configured, Copycat will perform state machine replication over the event bus using the event
        // bus addresses provided by the protocol URI - e.g. eventbus://foo
        // Because Copycat is a CP framework, we have to explicitly list all of the nodes in the cluster.
        ClusterConfig cluster = new ClusterConfig()
          .withProtocol(new VertxEventBusProtocol(vertx))
          .withMembers(((List<String>) members.toList()).stream()
            .collect(Collectors.mapping(member -> String.format("eventbus://%s", member), Collectors.toList())));

        // Configure Copycat with the event bus cluster and Vert.x event loop executor.
        CopycatConfig config = new CopycatConfig()
          .withClusterConfig(cluster)
          .withExecutor(new VertxEventLoopExecutor(vertx));

        // Add the state machine resource configuration with a Vert.x event loop executor to the Copycat configuration.
        // Configure the state machine with a persistent log that synchronously flushes to disk on every write.
        config.addStateMachineConfig("lock", new StateMachineConfig()
          .withLog(new FileLog()
            .withDirectory(new File(System.getProperty("java.io.tmpdir"), "lock"))
            .withFlushOnWrite(true)
            .withSegmentSize(1024 * 1024 * 32))
          .withExecutor(new VertxEventLoopExecutor(vertx)));

        // Create and open a new Copycat instance. The Copycat instance controls the cluster of verticles and manages
        // resources within the cluster - in this case just a single state machine.
        copycat = Copycat.create(String.format("eventbus://%s", id), config);

        // Once we create the Copycat instance, it needs to be opened. When the instance is opened, Copycat will begin
        // communicating with other nodes in the cluster and elect a leader that will control resources within the cluster.
        copycat.open().whenComplete((copycatResult, copycatError) -> {

          // Because we configured the Copycat instance with the VertxEventLoopExecutor Copycat executes
          // CompletableFuture callbacks on the Vert.x event loop, so we don't need to call runOnContext().
          if (copycatError != null) {
            startResult.setFailure(copycatError);
          } else {

            // Create and open a new state machine instance and create a lock proxy for submitting state machine
            // commands and queries to the cluster. When the state machine is opened, the state machine will communicate
            // with other nodes in the Copycat instance cluster to elect a leader and begin replicating the state log.
            copycat.stateMachine("lock", LockState.class, new UnlockedLockState()).open().whenComplete((stateMachine, error) -> {
              if (error != null) {
                startResult.setFailure(error);
              } else {
                this.stateMachine = stateMachine;
                this.lockProxy = stateMachine.createProxy(LockStateProxy.class);
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
      if (stateMachine != null) {
        stateMachine.close().get();
      }
      if (copycat != null) {
        copycat.close().get();
      }
    } catch (InterruptedException | ExecutionException e) {
    }
  }

  @Override
  public void handle(Message<JsonObject> message) {
    String action = message.body().getString("action");
    if (action != null) {
      switch (action) {
        case "lock":
          doLock(message);
          break;
        case "unlock":
          doUnlock(message);
          break;
      }
    }
  }

  /**
   * Locks the lock.
   */
  private void doLock(Message<JsonObject> message) {
    // Lock the lock by calling the "lock" method on the state proxy. When the "lock" method is called, Copycat will
    // internally forward the lock request to the current state machine resource leader which will log and replicate
    // the lock request. Once the request has been replicated on a majority of the nodes in the resource's cluster,
    // the request will be applied to the state machine state (i.e. UnlockedLockState or LockedLockState) and respond
    // with the result.
    lockProxy.lock(message.body().getString("holder")).whenComplete((result, error) -> {
      if (error != null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", error.getMessage()));
      } else {
        message.reply(new JsonObject().putString("status", "ok"));
      }
    });
  }

  /**
   * Unlocks the lock.
   */
  private void doUnlock(Message<JsonObject> message) {
    // Just as with locking the lock, we simply call the "unlock" method on the lock proxy to forward the lock request
    // to the current state machine resource leader which will replicate the command and apply it to the state machine.
    lockProxy.unlock(message.body().getString("holder")).whenComplete((result, error) -> {
      if (error != null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", error.getMessage()));
      } else {
        message.reply(new JsonObject().putString("status", "ok"));
      }
    });
  }

}
