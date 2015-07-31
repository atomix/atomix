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
package net.kuujo.copycat.raft.transport;

import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.transport.Client;
import net.kuujo.copycat.io.transport.Server;
import net.kuujo.copycat.io.transport.Transport;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local transport.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalTransport implements Transport {

  /**
   * Returns a new local transport builder.
   *
   * @return A new local transport builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final LocalServerRegistry registry;
  private final Serializer serializer;
  private final Map<UUID, LocalClient> clients = new ConcurrentHashMap<>();
  private final Map<UUID, LocalServer> servers = new ConcurrentHashMap<>();

  private LocalTransport(LocalServerRegistry registry, Serializer serializer) {
    this.registry = registry;
    this.serializer = serializer;
  }

  @Override
  public Client client(UUID id) {
    return clients.computeIfAbsent(id, i -> new LocalClient(id, registry, serializer));
  }

  @Override
  public Server server(UUID id) {
    return servers.computeIfAbsent(id, i -> new LocalServer(id, registry, serializer));
  }

  @Override
  public CompletableFuture<Void> close() {
    int i = 0;

    CompletableFuture[] futures = new CompletableFuture[clients.size() + servers.size()];
    for (Client client : clients.values()) {
      futures[i++] = client.close();
    }

    for (Server server : servers.values()) {
      futures[i++] = server.close();
    }

    return CompletableFuture.allOf(futures);
  }

  /**
   * Local transport builder.
   */
  public static class Builder extends Transport.Builder {
    private LocalServerRegistry registry;
    private Serializer serializer;

    /**
     * Sets the transport server registry.
     *
     * @param registry The local server registry.
     * @return The transport builder.
     */
    public Builder withRegistry(LocalServerRegistry registry) {
      if (registry == null)
        throw new NullPointerException("registry cannot be null");
      this.registry = registry;
      return this;
    }

    /**
     * Sets the transport serializer.
     *
     * @param serializer The transport serializer.
     * @return The transport builder.
     */
    public Builder withSerializer(Serializer serializer) {
      this.serializer = serializer;
      return this;
    }

    @Override
    public Transport build() {
      return new LocalTransport(registry, serializer != null ? serializer : new Serializer());
    }
  }

}
