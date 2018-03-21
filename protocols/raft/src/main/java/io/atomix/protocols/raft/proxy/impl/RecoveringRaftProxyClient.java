/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.proxy.impl;

import com.google.common.collect.Sets;
import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.event.RaftEvent;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.proxy.RaftProxyClient;
import io.atomix.protocols.raft.service.ServiceRevision;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.OrderedFuture;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.Scheduler;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Raft proxy that supports recovery.
 */
public class RecoveringRaftProxyClient implements RaftProxyClient {
  private static final SessionId DEFAULT_SESSION_ID = SessionId.from(0);
  private final String name;
  private final ServiceType serviceType;
  private final ServiceRevision revision;
  private final RaftProxyClient.Builder proxyClientBuilder;
  private final Scheduler scheduler;
  private Logger log;
  private volatile OrderedFuture<RaftProxyClient> clientFuture;
  private volatile RaftProxyClient client;
  private volatile RaftProxy.State state = RaftProxy.State.SUSPENDED;
  private final Set<Consumer<RaftProxy.State>> stateChangeListeners = Sets.newCopyOnWriteArraySet();
  private final Set<Consumer<RaftEvent>> eventListeners = Sets.newCopyOnWriteArraySet();
  private Scheduled recoverTask;
  private volatile boolean open = false;

  public RecoveringRaftProxyClient(
      String clientId,
      String name,
      ServiceType serviceType,
      ServiceRevision revision,
      RaftProxyClient.Builder proxyClientBuilder,
      Scheduler scheduler) {
    this.name = checkNotNull(name);
    this.serviceType = checkNotNull(serviceType);
    this.revision = checkNotNull(revision);
    this.proxyClientBuilder = checkNotNull(proxyClientBuilder);
    this.scheduler = checkNotNull(scheduler);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftClient.class)
        .addValue(clientId)
        .build());
  }

  @Override
  public SessionId sessionId() {
    RaftProxyClient client = this.client;
    return client != null ? client.sessionId() : DEFAULT_SESSION_ID;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public ServiceType serviceType() {
    return serviceType;
  }

  @Override
  public ServiceRevision revision() {
    RaftProxyClient client = this.client;
    return client != null ? client.revision() : revision;
  }

  @Override
  public RaftProxy.State getState() {
    return state;
  }

  /**
   * Sets the session state.
   *
   * @param state the session state
   */
  private synchronized void onStateChange(RaftProxy.State state) {
    if (this.state != state) {
      if (state == RaftProxy.State.CLOSED) {
        if (open) {
          onStateChange(RaftProxy.State.SUSPENDED);
          recover();
        } else {
          log.debug("State changed: {}", state);
          this.state = state;
          stateChangeListeners.forEach(l -> l.accept(state));
        }
      } else {
        log.debug("State changed: {}", state);
        this.state = state;
        stateChangeListeners.forEach(l -> l.accept(state));
      }
    }
  }

  @Override
  public void addStateChangeListener(Consumer<RaftProxy.State> listener) {
    stateChangeListeners.add(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<RaftProxy.State> listener) {
    stateChangeListeners.remove(listener);
  }

  /**
   * Verifies that the client is open.
   */
  private void checkOpen() {
    checkState(isOpen(), "client not open");
  }

  /**
   * Recovers the client.
   */
  private void recover() {
    client = null;
    openClient();
  }

  /**
   * Opens a new client.
   *
   * @return a future to be completed once the client has been opened
   */
  private CompletableFuture<RaftProxyClient> openClient() {
    if (open) {
      log.debug("Opening proxy session");

      clientFuture = new OrderedFuture<>();
      openClient(clientFuture);

      return clientFuture.thenApply(client -> {
        synchronized (this) {
          this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftProxy.class)
              .addValue(client.sessionId())
              .add("type", client.serviceType())
              .add("name", client.name())
              .build());
          this.client = client;
          client.addStateChangeListener(this::onStateChange);
          eventListeners.forEach(client::addEventListener);
          onStateChange(RaftProxy.State.CONNECTED);
        }
        return client;
      });
    }
    return Futures.exceptionalFuture(new IllegalStateException("Client not open"));
  }

  /**
   * Opens a new client, completing the provided future only once the client has been opened.
   *
   * @param future the future to be completed once the client is opened
   */
  private void openClient(CompletableFuture<RaftProxyClient> future) {
    proxyClientBuilder.buildAsync().whenComplete((client, error) -> {
      if (error == null) {
        future.complete(client);
      } else {
        recoverTask = scheduler.schedule(Duration.ofSeconds(1), () -> openClient(future));
      }
    });
  }

  @Override
  public CompletableFuture<byte[]> execute(RaftOperation operation) {
    checkOpen();
    RaftProxyClient client = this.client;
    if (client != null) {
      return client.execute(operation);
    } else {
      return clientFuture.thenCompose(c -> c.execute(operation));
    }
  }

  @Override
  public synchronized void addEventListener(Consumer<RaftEvent> consumer) {
    checkOpen();
    eventListeners.add(consumer);
    RaftProxyClient client = this.client;
    if (client != null) {
      client.addEventListener(consumer);
    }
  }

  @Override
  public synchronized void removeEventListener(Consumer<RaftEvent> consumer) {
    checkOpen();
    eventListeners.remove(consumer);
    RaftProxyClient client = this.client;
    if (client != null) {
      client.removeEventListener(consumer);
    }
  }

  @Override
  public synchronized CompletableFuture<RaftProxyClient> open() {
    if (!open) {
      open = true;
      return openClient().thenApply(c -> this);
    }
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (open) {
      open = false;
      if (recoverTask != null) {
        recoverTask.cancel();
      }

      RaftProxyClient client = this.client;
      if (client != null) {
        return client.close();
      } else {
        return clientFuture.thenCompose(c -> c.close());
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", client.name())
        .add("serviceType", client.serviceType())
        .add("state", state)
        .toString();
  }
}