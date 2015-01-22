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

import net.kuujo.copycat.cluster.coordinator.MemberCoordinator;
import net.kuujo.copycat.internal.cluster.MemberInfo;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Default remote member coordinator implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultRemoteMemberCoordinator extends AbstractMemberCoordinator {
  private final ProtocolClient client;
  private final Executor executor;

  public DefaultRemoteMemberCoordinator(MemberInfo info, Protocol protocol, Executor executor) {
    super(info);
    try {
      URI realUri = new URI(info.uri());
      if (!protocol.isValidUri(realUri)) {
        throw new ProtocolException(String.format("Invalid protocol URI %s", info.uri()));
      }
      this.client = protocol.createClient(realUri);
    } catch (URISyntaxException e) {
      throw new ProtocolException(e);
    }
    this.executor = executor;
  }

  @Override
  public CompletableFuture<ByteBuffer> send(String topic, int address, int id, ByteBuffer message) {
    return CompletableFuture.supplyAsync(() -> {
      ByteBuffer request = ByteBuffer.allocateDirect(message.capacity() + 12);
      request.putInt(topic.hashCode());
      request.putInt(address);
      request.putInt(id);
      request.put(message);
      request.flip();
      return request;
    }, executor)
      .thenCompose(client::write);
  }

  @Override
  public CompletableFuture<MemberCoordinator> open() {
    return super.open().thenComposeAsync(v -> client.connect(), executor).thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenComposeAsync(v -> client.close(), executor);
  }

  @Override
  public String toString() {
    return String.format("%s[uri=%s]", getClass().getCanonicalName(), uri());
  }

}
