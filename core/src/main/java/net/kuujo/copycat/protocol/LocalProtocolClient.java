/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.util.concurrent.NamedThreadFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Local protocol client implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocolClient implements ProtocolClient {
  private final Executor executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-protocol-thread-%d"));
  private final String address;
  private final Map<String, LocalProtocolServer> registry;

  LocalProtocolClient(String address, Map<String, LocalProtocolServer> registry) {
    this.address = address;
    this.registry = registry;
  }

  @Override
  public CompletableFuture<ByteBuffer> write(ByteBuffer request) {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    executor.execute(() -> {
      LocalProtocolServer server = registry.get(address);
      if (server != null) {
        server.handle(request).whenComplete((response, error) -> {
          if (error != null) {
            executor.execute(() -> future.completeExceptionally(error));
          } else {
            executor.execute(() -> future.complete(response));
          }
        });
      } else {
        future.completeExceptionally(new ProtocolException(String.format("Invalid server address %s", address)));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> connect() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

}
