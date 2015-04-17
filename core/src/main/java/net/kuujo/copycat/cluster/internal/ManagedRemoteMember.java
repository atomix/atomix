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
package net.kuujo.copycat.cluster.internal;

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.BufferPool;
import net.kuujo.copycat.io.HeapBuffer;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolConnection;
import net.kuujo.copycat.resource.PartitionContext;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Default remote member implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ManagedRemoteMember extends ManagedMember<Member> implements Member {
  private static final byte MESSAGE = 1;
  private static final byte USER = 0;
  private static final byte INTERNAL = 1;
  private static final byte TASK = 2;
  private final ProtocolClient client;
  private final BufferPool bufferPool = new HeapBufferPool();
  private ProtocolConnection connection;
  private final Map<String, Long> hashMap = new HashMap<>();
  private boolean open;

  public ManagedRemoteMember(int id, String address, Protocol protocol, PartitionContext context) {
    super(id, context);
    if (address == null)
      throw new NullPointerException("address cannot be null");
    try {
      this.client = protocol.createClient(new URI(address));
    } catch (URISyntaxException e) {
      throw new ConfigurationException("Invalid protocol URI");
    }
  }

  @Override
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    if (connection == null) {
      return client.connect()
        .thenAcceptAsync(c -> this.connection = c, context.getScheduler())
        .thenCompose(v -> doSend(topic, message));
    } else {
      return doSend(topic, message);
    }
  }

  /**
   * Sends a message to the remote member.
   */
  private <T, U> CompletableFuture<U> doSend(String topic, T message) {
    Buffer serialized = context.getSerializer().writeObject(message);
    Buffer request = bufferPool.acquire()
      .writeByte(MESSAGE)
      .writeByte(USER)
      .writeLong(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash64(t.getBytes())))
      .write(serialized)
      .flip();
    return connection.write(request).thenApplyAsync(b -> {
      request.close();
      return context.getSerializer().readObject(b);
    }, context.getExecutor());
  }

  /**
   * Sends an internal message.
   */
  public CompletableFuture<Buffer> sendInternal(Buffer message) {
    if (connection == null) {
      return client.connect()
        .thenAcceptAsync(c -> this.connection = c, context.getScheduler())
        .thenCompose(v -> doSendInternal(message));
    } else {
      return doSendInternal(message);
    }
  }

  /**
   * Sends an internal message.
   */
  private CompletableFuture<Buffer> doSendInternal(Buffer message) {
    Buffer request = HeapBuffer.allocate(message.limit() + 10)
      .writeByte(MESSAGE)
      .writeByte(INTERNAL)
      .write(message)
      .flip();
    return connection.write(request).thenApplyAsync(v -> v, context.getScheduler());
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return submit(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    if (connection == null) {
      return client.connect()
        .thenAcceptAsync(c -> this.connection = c, context.getScheduler())
        .thenCompose(v -> doSubmit(task));
    } else {
      return doSubmit(task);
    }
  }

  /**
   * Submits a task for remote execution.
   */
  private <T> CompletableFuture<T> doSubmit(Task<T> task) {
    Buffer serialized = context.getSerializer().writeObject(task);
    Buffer request = HeapBuffer.allocate(serialized.limit() + 1).writeByte(TASK).write(serialized).flip();
    return connection.write(request).thenApplyAsync(b -> context.getSerializer().readObject(b), context.getExecutor());
  }

  @Override
  public CompletableFuture<Member> open() {
    open = true;
    return super.open();
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    bufferPool.close();
    return super.close()
      .thenCompose(v -> connection != null ? client.close() : CompletableFuture.completedFuture(null))
      .thenRunAsync(() -> this.connection = null, context.getScheduler());
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
