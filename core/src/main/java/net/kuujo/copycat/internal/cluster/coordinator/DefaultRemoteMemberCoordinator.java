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
package net.kuujo.copycat.internal.cluster.coordinator;

import net.kuujo.copycat.Task;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;
import net.kuujo.copycat.util.serializer.Serializer;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Default remote member coordinator implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultRemoteMemberCoordinator extends AbstractMemberCoordinator {
  private static final int USER_ADDRESS = -1;
  private static final int SYSTEM_ADDRESS = 0;
  private final ProtocolClient client;
  private final ExecutionContext context;
  private final Serializer serializer = Serializer.serializer();

  DefaultRemoteMemberCoordinator(String uri, Protocol protocol, ExecutionContext context) {
    super(uri);
    try {
      URI realUri = new URI(uri);
      if (!protocol.isValidUri(realUri)) {
        throw new ProtocolException(String.format("Invalid protocol URI %s", uri));
      }
      this.client = protocol.createClient(realUri);
    } catch (URISyntaxException e) {
      throw new ProtocolException(e);
    }
    this.context = context;
  }

  @Override
  public <T, U> CompletableFuture<U> send(String topic, int address, T message) {
    CompletableFuture<U> future = new CompletableFuture<>();
    ByteBuffer buffer = serializer.writeObject(message);
    byte[] topicBytes = topic.getBytes();
    ByteBuffer request = ByteBuffer.allocateDirect(4 + buffer.capacity() + topicBytes.length + 8);
    request.putInt(1); // Request type
    request.putInt(topicBytes.length);
    request.put(topicBytes);
    request.putInt(address);
    request.put(buffer);
    client.write(request).whenComplete((response, error) -> {
      if (error == null) {
        context.execute(() -> future.complete(serializer.readObject(response)));
      } else {
        context.execute(() -> future.completeExceptionally(error));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> execute(int address, Task<Void> task) {
    return submit(address, task);
  }

  @Override
  public <T> CompletableFuture<T> submit(int address, Task<T> task) {
    CompletableFuture<T> future = new CompletableFuture<>();
    ByteBuffer buffer = serializer.writeObject(task);
    ByteBuffer request = ByteBuffer.allocateDirect(8 + buffer.capacity());
    request.putInt(0); // Request type
    request.putInt(address); // Context address
    request.put(buffer);
    client.write(request).whenComplete((response, error) -> {
      if (error == null) {
        context.execute(() -> future.complete(serializer.readObject(response)));
      } else {
        context.execute(() -> future.completeExceptionally(error));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> open() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    client.connect().whenComplete((result, error) -> {
      context.execute(() -> {
        if (error == null) {
          future.complete(null);
        } else {
          future.completeExceptionally(error);
        }
      });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    client.close().whenComplete((result, error) -> {
      context.execute(() -> {
        if (error == null) {
          future.complete(null);
        } else {
          future.completeExceptionally(error);
        }
      });
    });
    return future;
  }

}
