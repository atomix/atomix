/*
 * Copyright 2017-present Open Networking Laboratory
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
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.session.SessionId;
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

/**
 * Raft proxy that supports recovery.
 */
public class RecoveringRaftProxyClient implements RaftProxyClient {
  private static final SessionId DEFAULT_SESSION_ID = SessionId.from(0);
  private final String name;
  private final ServiceType serviceType;
  private final RaftProxyClient.Builder proxyClientBuilder;
  private final Scheduler scheduler;
  private Logger log;
  private volatile OrderedFuture<RaftProxyClient> openFuture;
  private volatile RaftProxyClient client;
  private volatile RaftProxy.State state = State.SUSPENDED;
  private final Set<Consumer<State>> stateChangeListeners = Sets.newCopyOnWriteArraySet();
  private final Set<Consumer<RaftEvent>> eventListeners = Sets.newCopyOnWriteArraySet();
  private Scheduled recoverTask;
  private boolean recover = true;

  public RecoveringRaftProxyClient(String clientId, String name, ServiceType serviceType, RaftProxyClient.Builder proxyClientBuilder, Scheduler scheduler) {
    this.name = checkNotNull(name);
    this.serviceType = checkNotNull(serviceType);
    this.proxyClientBuilder = checkNotNull(proxyClientBuilder);
    this.scheduler = checkNotNull(scheduler);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftClient.class)
        .addValue(clientId)
        .build());
    recover();
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
  public State getState() {
    return state;
  }

  /**
   * Sets the session state.
   *
   * @param state the session state
   */
  private synchronized void onStateChange(State state) {
    if (this.state != state) {
      if (state == State.CLOSED) {
        if (recover) {
          onStateChange(State.SUSPENDED);
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
  public void addStateChangeListener(Consumer<State> listener) {
    stateChangeListeners.add(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<State> listener) {
    stateChangeListeners.remove(listener);
  }

  /**
   * Recovers the proxy client.
   */
  private synchronized void recover() {
    if (recover) {
      // Setting the client to null will force operations to be queued until a new client is connected
      client = null;

      // Store the open future so operations can be enqueued waiting for the new client to be connected
      openFuture = openClient();

      // Once the new client is connected, update the logger/listeners.
      openFuture.thenAccept(client -> {
        synchronized (this) {
          this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftProxy.class)
              .addValue(client.sessionId())
              .add("type", client.serviceType())
              .add("name", client.name())
              .build());
          this.client = client;
          client.addStateChangeListener(this::onStateChange);
          eventListeners.forEach(client::addEventListener);
          onStateChange(State.CONNECTED);
        }
      });
    }
  }

  /**
   * Opens a new client.
   *
   * @return a future to be completed once the client has been opened
   */
  private OrderedFuture<RaftProxyClient> openClient() {
    OrderedFuture<RaftProxyClient> future = new OrderedFuture<>();
    openClient(future);
    return future;
  }

  /**
   * Opens a new client, completing the provided future only once the client has been opened.
   *
   * @param future the future to be completed once the client is opened
   */
  private void openClient(CompletableFuture<RaftProxyClient> future) {
    log.debug("Opening session");
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
    RaftProxyClient client = this.client;
    if (client != null) {
      return client.execute(operation);
    } else {
      return openFuture.thenCompose(c -> c.execute(operation));
    }
  }

  @Override
  public synchronized void addEventListener(Consumer<RaftEvent> consumer) {
    eventListeners.add(consumer);
    RaftProxyClient client = this.client;
    if (client != null) {
      client.addEventListener(consumer);
    }
  }

  @Override
  public synchronized void removeEventListener(Consumer<RaftEvent> consumer) {
    eventListeners.remove(consumer);
    RaftProxyClient client = this.client;
    if (client != null) {
      client.removeEventListener(consumer);
    }
  }

  @Override
  public boolean isOpen() {
    return state == State.CONNECTED;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    recover = false;
    if (recoverTask != null) {
      recoverTask.cancel();
    }

    RaftProxyClient client = this.client;
    if (client != null) {
      return client.close();
    } else {
      return openFuture.thenCompose(c -> c.close());
    }
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