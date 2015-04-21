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
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.cluster.protocol.Client;
import net.kuujo.copycat.cluster.protocol.Connection;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.BufferPool;
import net.kuujo.copycat.io.HeapBuffer;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.util.concurrent.CopycatThreadFactory;
import net.kuujo.copycat.util.concurrent.ExecutionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/**
 * Default remote member implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RemoteMember extends Member {

  /**
   * Returns a new remote member builder.
   *
   * @return A new remote member builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final byte MESSAGE = 0;
  private static final byte TASK = 1;
  private final Client client;
  private BufferPool bufferPool;
  private Connection connection;
  private final Map<String, Integer> hashMap = new HashMap<>();
  private CompletableFuture<RemoteMember> connectFuture;
  private CompletableFuture<Void> closeFuture;
  private boolean open;

  public RemoteMember(Info info, Client client, CopycatSerializer serializer, ExecutionContext context) {
    super(info, serializer, context);
    this.client = client;
  }

  @Override
  public String address() {
    return client.address();
  }

  @Override
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    if (connection == null) {
      return client.connect()
        .thenAcceptAsync(c -> this.connection = c, context)
        .thenCompose(v -> doSend(topic, message));
    } else {
      return doSend(topic, message);
    }
  }

  /**
   * Sends a message to the remote member.
   */
  private synchronized <T, U> CompletableFuture<U> doSend(String topic, T message) {
    ExecutionContext context = getContext();
    Buffer serialized = serializer.writeObject(message);
    Buffer request = bufferPool.acquire()
      .writeByte(MESSAGE)
      .writeLong(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash32(t.getBytes())))
      .write(serialized)
      .flip();
    return connection.write(request).thenApplyAsync(b -> {
      request.close();
      return serializer.readObject(b);
    }, context);
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return submit(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    if (connection == null) {
      return client.connect()
        .thenAcceptAsync(c -> this.connection = c, context)
        .thenCompose(v -> doSubmit(task));
    } else {
      return doSubmit(task);
    }
  }

  /**
   * Submits a task for remote execution.
   */
  private synchronized <T> CompletableFuture<T> doSubmit(Task<T> task) {
    ExecutionContext context = getContext();
    Buffer serialized = serializer.writeObject(task);
    Buffer request = HeapBuffer.allocate(serialized.limit() + 1).writeByte(TASK).write(serialized).flip();
    return connection.write(request).thenApplyAsync(serializer::readObject, context);
  }

  /**
   * Connects the remote client.
   *
   * @return A completable future to be called once the client is connected.
   */
  public CompletableFuture<RemoteMember> connect() {
    if (open)
      return CompletableFuture.completedFuture(this);

    if (connectFuture == null) {
      synchronized (this) {
        if (connectFuture == null) {
          connectFuture = client.connect()
            .thenAcceptAsync(c -> {
              bufferPool = new HeapBufferPool();
              connection = c;
              info.address = client.address();
              open = true;
            }, context)
            .thenApply(v -> this);
        }
      }
    }
    return connectFuture;
  }

  /**
   * Closes the remote client.
   *
   * @return A completable future to be called once the client is closed.
   */
  public CompletableFuture<Void> close() {
    if (!open)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = connection.close().thenRun(() -> {
            bufferPool.close();
            bufferPool = null;
            connection = null;
            info.address = null;
            open = false;
          });
        }
      }
    }
    return closeFuture;
  }

  /**
   * Remote member builder.
   */
  public static class Builder extends Member.Builder<Builder> {
    private Client client;

    private Builder() {
    }

    /**
     * Sets the remote member client.
     *
     * @param client The remote member client.
     * @return The remote member builder.
     */
    public Builder withClient(Client client) {
      this.client = client;
      return this;
    }

    @Override
    public RemoteMember build() {
      return new RemoteMember(new Info(id, type), client, serializer != null ? serializer : new CopycatSerializer(), context != null ? context : new ExecutionContext(Executors.newSingleThreadScheduledExecutor(new CopycatThreadFactory("remote-member-" + id))));
    }
  }

}
