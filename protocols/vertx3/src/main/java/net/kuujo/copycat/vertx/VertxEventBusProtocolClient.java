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
package net.kuujo.copycat.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolConnection;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Vert.x event bus protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusProtocolClient implements ProtocolClient {
  private final String address;
  private final Vertx vertx;
  private final Context context;
  private final Map<String, ProtocolConnection> connections = new ConcurrentHashMap<>(1024);

  public VertxEventBusProtocolClient(String address, Vertx vertx) {
    if (address == null)
      throw new NullPointerException("address cannot be null");
    if (vertx == null)
      throw new NullPointerException("vertx cannot be null");
    this.address = address;
    this.vertx = vertx;
    this.context = vertx.getOrCreateContext();
  }

  @Override
  public String address() {
    return String.format("eventbus://%s", address);
  }

  @Override
  public CompletableFuture<ProtocolConnection> connect() {
    CompletableFuture<ProtocolConnection> future = new CompletableFuture<>();
    String address = UUID.randomUUID().toString();
    new VertxEventBusProtocolConnection(address, vertx, connectionResult -> {
      if (connectionResult.failed()) {
        future.completeExceptionally(connectionResult.cause());
      } else {
        VertxEventBusProtocolConnection connection = connectionResult.result();
        connections.put(address, connection);
        context.runOnContext(v -> {
          vertx.eventBus().send(this.address, address, new DeliveryOptions().setSendTimeout(5000).addHeader("action", "connect"), (Handler<AsyncResult<Message<JsonObject>>>) connectResult -> {
            if (connectResult.failed()) {
              connection.close().whenComplete((closeResult, closeError) -> {
                future.completeExceptionally(connectResult.cause());
              });
            } else {
              connection.address(connectResult.result().body().getString("address"));
              future.complete(connection);
            }
          });
        });
      }
    });
    return future;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    CompletableFuture<Void>[] futures = new CompletableFuture[connections.size()];
    int i = 0;
    for (Map.Entry<String, ProtocolConnection> entry : connections.entrySet()) {
      futures[i++] = entry.getValue().close().thenRun(() -> connections.remove(entry.getKey()));
    }
    return CompletableFuture.allOf(futures);
  }

  @Override
  public String toString() {
    return String.format("%s[address=%s]", getClass().getSimpleName(), address);
  }

}
