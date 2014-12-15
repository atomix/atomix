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

import net.kuujo.copycat.internal.ThreadExecutionContext;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.spi.ExecutionContext;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Local protocol server implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocolServer implements ProtocolServer {
  private final ExecutionContext context = new ThreadExecutionContext(new NamedThreadFactory("copycat-protocol-thread-%d"));
  private final String address;
  private final Map<String, LocalProtocolServer> registry;
  private ProtocolHandler handler;

  LocalProtocolServer(String address, Map<String, LocalProtocolServer> registry) {
    this.address = address;
    this.registry = registry;
  }

  @Override
  public void handler(ProtocolHandler handler) {
    this.handler = handler;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  CompletableFuture<ByteBuffer> handle(ByteBuffer request) {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    context.execute(() -> {
      if (handler == null) {
        future.completeExceptionally(new ProtocolException("No protocol handler registered"));
      } else {
        handler.handle(request).whenComplete((result, error) -> {
          if (error == null) {
            result.rewind();
            future.complete(result);
          } else {
            future.completeExceptionally(error);
          }
        });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> listen() {
    return context.submit(() -> {
      registry.put(address, this);
      return null;
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    return context.submit(() -> {
      registry.remove(address);
      return null;
    });
  }

}
