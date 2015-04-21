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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.MembershipEvent;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.raft.RaftConfig;
import net.kuujo.copycat.raft.RaftContext;
import net.kuujo.copycat.raft.RaftMember;
import net.kuujo.copycat.raft.protocol.*;
import net.kuujo.copycat.raft.storage.RaftStorage;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.util.concurrent.ExecutionContext;
import net.kuujo.copycat.util.concurrent.Futures;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * Raft protocol implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftProtocol extends RaftContext implements Protocol {

  /**
   * Returns a new Raft protocol builder.
   *
   * @return A new Raft protocol builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private Resource resource;
  private Cluster cluster;
  private net.kuujo.copycat.protocol.ProtocolHandler handler;
  private CompletableFuture<Void> openFuture;
  private CompletableFuture<Void> closeFuture;
  private boolean open;

  private RaftProtocol(RaftStorage storage, RaftConfig config, ScheduledExecutorService executor) {
    super(storage, config, executor);
  }

  @Override
  protected Buffer commit(Buffer key, Buffer entry, Buffer result) {
    return handler.handle(key, entry, result);
  }

  @Override
  protected <T extends Request, U extends Response> CompletableFuture<U> sendRequest(T request, RaftMember member) {
    Member sendMember = cluster.member(member.id());
    if (sendMember == null)
      return Futures.exceptionalFuture(new ProtocolException("unknown member %d", member.id()));
    return sendMember.send(resource.name(), request);
  }

  @Override
  public Protocol handler(net.kuujo.copycat.protocol.ProtocolHandler handler) {
    this.handler = handler;
    return this;
  }

  /**
   * Membership change event handler.
   */
  private synchronized void membershipChanged(MembershipEvent event) {
    if (event.member().type() != Member.Type.ACTIVE) {
      if (event.type() == MembershipEvent.Type.JOIN) {
        RaftMember member = getMember(event.member().id());
        if (member == null) {
          member = RaftMember.builder()
            .withId(event.member().id())
            .withType(event.member().type() == Member.Type.PASSIVE ? RaftMember.Type.PASSIVE : RaftMember.Type.REMOTE)
            .build();
          addMember(member);
        }
      } else if (event.type() == MembershipEvent.Type.LEAVE) {
        RaftMember member = getMember(event.member().id());
        if (member != null) {
          removeMember(member);
        }
      }
    }
  }

  @Override
  public CompletableFuture<Buffer> read(Buffer key, Buffer entry, Consistency consistency) {
    if (!open)
      throw new IllegalStateException("protocol not open");

    CompletableFuture<Buffer> future = new CompletableFuture<>();
    ReadRequest request = ReadRequest.builder()
      .withKey(key)
      .withEntry(entry)
      .withConsistency(consistency)
      .build();
    apply(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete(response.asReadResponse().result());
        } else {
          future.completeExceptionally(response.error().createException());
        }
      } else {
        future.completeExceptionally(error);
      }
      request.close();
    });
    return future;
  }

  @Override
  public CompletableFuture<Buffer> write(Buffer key, Buffer entry, Consistency consistency) {
    if (!open)
      throw new IllegalStateException("protocol not open");

    CompletableFuture<Buffer> future = new CompletableFuture<>();
    WriteRequest request = WriteRequest.builder()
      .withKey(key)
      .withEntry(entry)
      .build();
    apply(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete(response.asWriteResponse().result());
        } else {
          future.completeExceptionally(response.error().createException());
        }
      } else {
        future.completeExceptionally(error);
      }
      request.close();
    });
    return future;
  }

  @Override
  public CompletableFuture<Buffer> delete(Buffer key, Buffer entry, Consistency consistency) {
    if (!open)
      throw new IllegalStateException("protocol not open");

    CompletableFuture<Buffer> future = new CompletableFuture<>();
    DeleteRequest request = DeleteRequest.builder()
      .withKey(key)
      .build();
    apply(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete(response.asDeleteResponse().result());
        } else {
          future.completeExceptionally(response.error().createException());
        }
      } else {
        future.completeExceptionally(error);
      }
      request.close();
    });
    return future;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> open(Resource resource) {
    if (open)
      return CompletableFuture.completedFuture(null);

    if (openFuture == null) {
      synchronized (this) {
        if (openFuture == null) {
          this.resource = resource;
          cluster = resource.cluster();
          openFuture = cluster.open()
            .thenCompose(v -> {
              cluster.addMembershipListener(this::membershipChanged);
              cluster.members().stream()
                .filter(m -> m.type() == Member.Type.ACTIVE)
                .forEach(m -> addMember(RaftMember.builder().withId(m.id()).withType(RaftMember.Type.ACTIVE).build()));
              return super.open();
            })
            .thenApply(v -> {
              cluster.members().stream()
                .filter(m -> m.type() != Member.Type.ACTIVE)
                .forEach(m -> addMember(RaftMember.builder()
                  .withId(m.id())
                  .withType(m.type() == Member.Type.PASSIVE ? RaftMember.Type.PASSIVE : RaftMember.Type.REMOTE)
                  .build()));
              cluster.member().registerHandler(resource.name(), this::handleRequest);
              return null;
            });
        }
      }
    }
    return openFuture;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!open)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          cluster.removeMembershipListener(this::membershipChanged);
          cluster.member().unregisterHandler(resource.name());
          closeFuture = super.close()
            .thenCompose(v -> cluster.close())
            .thenRun(() -> open = false);
        }
      }
    }
    return closeFuture;
  }

  /**
   * Raft protocol builder.
   */
  public static class Builder {
    private RaftStorage storage;
    private final RaftConfig config = new RaftConfig();
    private ExecutionContext context;

    /**
     * Sets the Raft storage.
     *
     * @param storage The Raft storage.
     * @return The Raft protocol builder.
     */
    public Builder withStorage(RaftStorage storage) {
      this.storage = storage;
      return this;
    }

    /**
     * Sets the Raft heartbeat interval.
     *
     * @param interval The Raft heartbeat interval.
     * @return The Raft protocol builder.
     */
    public Builder withHeartbeatInterval(long interval) {
      config.setHeartbeatInterval(interval);
      return this;
    }

    /**
     * Sets the Raft heartbeat interval.
     *
     * @param interval The Raft heartbeat interval.
     * @param unit The heartbeat time unit.
     * @return The Raft protocol builder.
     */
    public Builder withHeartbeatInterval(long interval, TimeUnit unit) {
      config.setHeartbeatInterval(interval, unit);
      return this;
    }

    /**
     * Sets the Raft election timeout.
     *
     * @param timeout The Raft election timeout.
     * @return The Raft protocol builder.
     */
    public Builder withElectionTimeout(long timeout) {
      config.setElectionTimeout(timeout);
      return this;
    }

    /**
     * Sets the Raft election timeout.
     *
     * @param timeout The Raft election timeout.
     * @param unit The election time unit.
     * @return The Raft protocol builder.
     */
    public Builder withElectionTimeout(long timeout, TimeUnit unit) {
      config.setElectionTimeout(timeout, unit);
      return this;
    }

    /**
     * Sets the execution context.
     *
     * @param context The execution context.
     * @return The Raft protocol builder.
     */
    public Builder withExecutionContext(ExecutionContext context) {
      this.context = context;
      return this;
    }

    /**
     * Builds the Raft protocol.
     *
     * @return The Raft protocol.
     */
    public RaftProtocol build() {
      return new RaftProtocol(storage, config, new WrappedExecutionContext(context));
    }
  }

  /**
   * Wrapped execution context.
   */
  private static class WrappedExecutionContext implements ScheduledExecutorService {
    private final ExecutionContext context;

    private WrappedExecutionContext(ExecutionContext context) {
      this.context = context;
    }

    @NotNull
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      return context.schedule(command, delay, unit);
    }

    @NotNull
    @Override
    @SuppressWarnings("unchecked")
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      return context.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @NotNull
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
      context.close();
    }

    @NotNull
    @Override
    public List<Runnable> shutdownNow() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isShutdown() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isTerminated() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <T> Future<T> submit(Callable<T> task) {
      throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Future<?> submit(Runnable task) {
      throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void execute(Runnable command) {
      context.execute(command);
    }
  }

}
