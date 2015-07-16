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
package net.kuujo.copycat.raft;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.copycat.transport.Transport;
import net.kuujo.copycat.raft.state.RaftClientState;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Raft client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftClient implements ManagedRaft {

  /**
   * Returns a new Raft client builder.
   *
   * @return A new Raft client builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final RaftClientState client;
  private CompletableFuture<Raft> openFuture;
  private CompletableFuture<Void> closeFuture;
  private volatile boolean open;

  private RaftClient(RaftClientState client) {
    this.client = client;
  }

  @Override
  public Sessions sessions() {
    return client.getSessions();
  }

  @Override
  public Session session() {
    return client.getSession();
  }

  @Override
  public <T> CompletableFuture<T> submit(Command<T> command) {
    if (!open)
      throw new IllegalStateException("protocol not open");
    return client.submit(command);
  }

  @Override
  public <T> CompletableFuture<T> submit(Query<T> query) {
    if (!open)
      throw new IllegalStateException("protocol not open");
    return client.submit(query);
  }

  @Override
  public CompletableFuture<Raft> open() {
    if (open)
      return CompletableFuture.completedFuture(this);

    if (openFuture == null) {
      synchronized (this) {
        if (openFuture == null) {
          if (closeFuture == null) {
            openFuture = client.open().thenApply(c -> {
              openFuture = null;
              open = true;
              return this;
            });
          } else {
            openFuture = closeFuture.thenCompose(v -> client.open().thenApply(c -> {
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
            closeFuture = client.close().thenRun(() -> {
              closeFuture = null;
              open = false;
            });
          } else {
            closeFuture = openFuture.thenCompose(c -> client.close().thenRun(() -> {
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
  public static class Builder implements Raft.Builder<Builder, RaftClient> {
    private static final Random RANDOM = new Random();
    private Transport transport;
    private Alleycat serializer;
    private long keepAliveInterval = 1000;
    private Members members;

    private Builder() {
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
      return new RaftClient(new RaftClientState(nextClientId(), transport, members, serializer).setKeepAliveInterval(keepAliveInterval));
    }

    /**
     * Returns a random client ID.
     *
     * @return A random client ID.
     */
    private static int nextClientId() {
      return RANDOM.nextInt(Integer.MAX_VALUE - 1023) + 1024;
    }
  }

}
