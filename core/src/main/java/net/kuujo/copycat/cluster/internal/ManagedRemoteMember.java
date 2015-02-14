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

import net.kuujo.copycat.Task;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolConnection;
import net.kuujo.copycat.raft.RaftMember;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.ConfigurationException;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
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
  private ProtocolConnection connection;

  public ManagedRemoteMember(RaftMember member, Protocol protocol, ResourceContext context) {
    super(member, context);
    try {
      this.client = protocol.createClient(new URI(member.uri()));
    } catch (URISyntaxException e) {
      throw new ConfigurationException("Invalid protocol URI");
    }
  }

  @Override
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    ByteBuffer serialized = context.serializer().writeObject(message);
    ByteBuffer request = ByteBuffer.allocate(serialized.limit() + 6);
    request.put(MESSAGE);
    request.put(USER);
    request.putInt(topic.hashCode());
    request.put(serialized);
    return connection.write(request).thenApplyAsync(b -> context.serializer().readObject(b), context.executor());
  }

  /**
   * Sends a message.
   */
  public CompletableFuture<ByteBuffer> sendInternal(String topic, ByteBuffer message) {
    ByteBuffer request = ByteBuffer.allocate(message.limit() + 6);
    request.put(MESSAGE);
    request.put(INTERNAL);
    request.putInt(topic.hashCode());
    request.put(message);
    request.flip();
    return connection.write(request);
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return submit(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    ByteBuffer serialized = context.serializer().writeObject(task);
    ByteBuffer request = ByteBuffer.allocate(serialized.limit() + 1);
    request.put(TASK);
    request.put(serialized);
    request.flip();
    return connection.write(request).thenApplyAsync(b -> context.serializer().readObject(b), context.executor());
  }

  @Override
  public CompletableFuture<Member> open() {
    return client.connect().thenAccept(c -> this.connection = c).thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return connection != null;
  }

  @Override
  public CompletableFuture<Void> close() {
    return client.close().thenRun(() -> this.connection = null);
  }

  @Override
  public boolean isClosed() {
    return connection == null;
  }

}
