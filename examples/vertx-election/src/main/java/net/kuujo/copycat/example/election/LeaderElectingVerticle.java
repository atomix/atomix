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
package net.kuujo.copycat.example.election;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.election.LeaderElectionConfig;
import net.kuujo.copycat.vertx.VertxEventBusProtocol;
import net.kuujo.copycat.vertx.VertxEventLoopExecutor;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.platform.Verticle;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Example verticle that performs an in-memory leader election.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaderElectingVerticle extends Verticle {
  private LeaderElection election;

  @Override
  public void start(final Future<Void> startResult) {
    String address = container.config().getString("address");
    JsonArray members = container.config().getArray("cluster");

    // Configure the Copycat cluster with the Vert.x event bus protocol and event bus members. With the event
    // bus protocol configured, Copycat will perform state machine replication over the event bus using the event
    // bus addresses provided by the protocol URI - e.g. eventbus://foo
    // Because Copycat is a CP framework, we have to explicitly list all of the nodes in the cluster.
    ClusterConfig cluster = new ClusterConfig()
      .withProtocol(new VertxEventBusProtocol(vertx))
      .withMembers(((List<String>) members.toList()).stream()
        .collect(Collectors.mapping(member -> String.format("eventbus://%s", member), Collectors.toList())));

    // Create a leader election configuration with a Vert.x event loop executor.
    LeaderElectionConfig config = new LeaderElectionConfig()
      .withExecutor(new VertxEventLoopExecutor(vertx));

    // Create and open the leader election using the constructed cluster configuration.
    LeaderElection.create("election", String.format("eventbus://%s", address), cluster, config).open().whenComplete((election, error) -> {
      // Since we configured the election with a Vert.x event loop executor, CompletableFuture callbacks are executed
      // on the Vert.x event loop, so we don't have to use runOnContext.
      if (error != null) {
        startResult.setFailure(error);
      } else {
        this.election = election;

        // Once the election has been opened, register a message handler on the election cluster to allow other members
        // of the cluster to send messages to us if we're the leader. Messages sent to this handler will be sent over
        // the Vert.x event bus since we're using the event bus protocol.
        election.cluster().member().<String, Void>registerHandler("print", message -> {
          System.out.println(message);
          return CompletableFuture.completedFuture(null);
        });

        // Finally, register an election listener. When a member is elected leader, log a message indicating who was
        // elected leader and send the leader a message to print.
        election.addListener(member -> {
          container.logger().info(member.uri() + " was elected leader!");
          member.send("print", "Hello world!").thenRun(() -> {
            container.logger().info("Leader printed 'Hello world!'");
          });
        });

        startResult.setResult(null);
      }
    });
  }

  @Override
  public void stop() {
    try {
      if (election != null) {
        election.close().get();
      }
    } catch (InterruptedException | ExecutionException e) {
    }
  }

}
