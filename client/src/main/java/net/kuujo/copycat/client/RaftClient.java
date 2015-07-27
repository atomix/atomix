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
package net.kuujo.copycat.client;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.ServiceLoaderResolver;
import net.kuujo.copycat.*;
import net.kuujo.copycat.client.state.ClientContext;
import net.kuujo.copycat.transport.Transport;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.concurrent.Context;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Raft client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftClient implements Raft, Managed<Raft> {

  /**
   * Returns a new Raft client builder.
   *
   * @return A new Raft client builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final ClientContext context;
  private CompletableFuture<Raft> openFuture;
  private CompletableFuture<Void> closeFuture;
  private volatile boolean open;

  protected RaftClient(ClientContext context) {
    this.context = context;
  }

  @Override
  public Context context() {
    return context.getContext();
  }

  @Override
  public Session session() {
    return context.getSession();
  }

  @Override
  public <T> CompletableFuture<T> submit(Command<T> command) {
    if (!open)
      throw new IllegalStateException("protocol not open");
    return context.submit(command);
  }

  @Override
  public <T> CompletableFuture<T> submit(Query<T> query) {
    if (!open)
      throw new IllegalStateException("protocol not open");
    return context.submit(query);
  }

  @Override
  public CompletableFuture<Raft> open() {
    if (open)
      return CompletableFuture.completedFuture(this);

    if (openFuture == null) {
      synchronized (this) {
        if (openFuture == null) {
          if (closeFuture == null) {
            openFuture = context.open().thenApply(c -> {
              openFuture = null;
              open = true;
              return this;
            });
          } else {
            openFuture = closeFuture.thenCompose(v -> context.open().thenApply(c -> {
              openFuture = null;
              open = true;
              return this;
            }));
          }
        }
      }
    }
    return openFuture;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!open)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          if (openFuture == null) {
            closeFuture = context.close().thenRun(() -> {
              closeFuture = null;
              open = false;
            });
          } else {
            closeFuture = openFuture.thenCompose(c -> context.close().thenRun(() -> {
              closeFuture = null;
              open = false;
            }));
          }
        }
      }
    }
    return closeFuture;
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  @Override
  public CompletableFuture<Void> delete() {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Raft client builder.
   */
  public static class Builder extends Raft.Builder<Builder, RaftClient> {
    private Transport transport;
    private Alleycat serializer;
    private long keepAliveInterval = 1000;
    private Members members;

    private Builder() {
    }

    @Override
    protected void reset() {
      transport = null;
      serializer = null;
      keepAliveInterval = 1000;
      members = null;
    }

    @Override
    public Builder withTransport(Transport transport) {
      this.transport = transport;
      return this;
    }

    /**
     * Sets the client serializer.
     *
     * @param serializer The client serializer.
     * @return The client builder.
     */
    public Builder withSerializer(Alleycat serializer) {
      this.serializer = serializer;
      return this;
    }

    /**
     * Sets the interval at which to send keep alive requests.
     *
     * @param keepAliveInterval The interval at which to send keep alive requests.
     * @return The client builder.
     */
    public Builder withKeepAliveInterval(long keepAliveInterval) {
      if (keepAliveInterval <= 0)
        throw new IllegalArgumentException("keep alive interval must be positive");
      this.keepAliveInterval = keepAliveInterval;
      return this;
    }

    /**
     * Sets the interval at which to send keep alive requests.
     *
     * @param keepAliveInterval The interval at which to send keep alive requests.
     * @param unit The keep alive interval time unit.
     * @return The client builder.
     */
    public Builder withKeepAliveInterval(long keepAliveInterval, TimeUnit unit) {
      return withKeepAliveInterval(unit.toMillis(keepAliveInterval));
    }

    /**
     * Sets the client seed members.
     *
     * @param members The client seed members.
     * @return The client builder.
     */
    public Builder withMembers(Members members) {
      this.members = members;
      return this;
    }

    @Override
    public RaftClient build() {
      // If no Alleycat instance was provided, create one.
      if (serializer == null) {
        serializer = new Alleycat();
      }

      // Resolve Alleycat serializable types with the ServiceLoaderResolver.
      serializer.resolve(new ServiceLoaderResolver());

      return new RaftClient(new ClientContext(members, transport, serializer).setKeepAliveInterval(keepAliveInterval));
    }
  }

}
