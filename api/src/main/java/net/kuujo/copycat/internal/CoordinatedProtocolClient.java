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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.ReferencePool;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolConnection;
import net.kuujo.copycat.protocol.ProtocolException;

import java.util.concurrent.CompletableFuture;

/**
 * Protocol client coordinator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedProtocolClient implements ProtocolClient {
  private final ReferencePool<Buffer> bufferPool = new HeapBufferPool();
  private final int id;
  private final ProtocolClient client;

  public CoordinatedProtocolClient(int id, ProtocolClient client) {
    this.id = id;
    this.client = client;
  }

  @Override
  public String address() {
    return client.address();
  }

  @Override
  public CompletableFuture<ProtocolConnection> connect() {
    CompletableFuture<ProtocolConnection> future = new CompletableFuture<>();
    client.connect().whenComplete((connection, connectionError) -> {
      if (connectionError == null) {
        connection.write(bufferPool.acquire().writeInt(id).flip()).whenComplete((response, responseError) -> {
          if (responseError == null) {
            int result = response.readByte();
            if (result == 1) {
              future.complete(connection);
            } else {
              future.completeExceptionally(new ProtocolException("Server not found"));
            }
          } else {
            future.completeExceptionally(responseError);
          }
        });
      } else {
        future.completeExceptionally(connectionError);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    return client.close();
  }

}
