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
package io.atomix.primitive.client.impl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.client.SessionClient;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.OrderedFuture;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primitive proxy that supports recovery.
 */
public class RecoveringSessionClient implements SessionClient {
  private static final SessionId DEFAULT_SESSION_ID = SessionId.from(0);
  private final PartitionId partitionId;
  private final String name;
  private final PrimitiveType primitiveType;
  private final Supplier<SessionClient> proxyFactory;
  private final ThreadContext context;
  private Logger log;
  private volatile OrderedFuture<SessionClient> clientFuture;
  private volatile SessionClient proxy;
  private volatile PrimitiveState state = PrimitiveState.CLOSED;
  private final Set<Consumer<PrimitiveState>> stateChangeListeners = Sets.newCopyOnWriteArraySet();
  private final Multimap<EventType, Consumer<PrimitiveEvent>> eventListeners = HashMultimap.create();
  private Scheduled recoverTask;
  private volatile boolean connected = false;

  public RecoveringSessionClient(
      String clientId,
      PartitionId partitionId,
      String name,
      PrimitiveType primitiveType,
      Supplier<SessionClient> proxyFactory,
      ThreadContext context) {
    this.partitionId = checkNotNull(partitionId);
    this.name = checkNotNull(name);
    this.primitiveType = checkNotNull(primitiveType);
    this.proxyFactory = checkNotNull(proxyFactory);
    this.context = checkNotNull(context);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(SessionClient.class)
        .addValue(clientId)
        .build());
  }

  @Override
  public SessionId sessionId() {
    SessionClient proxy = this.proxy;
    return proxy != null ? proxy.sessionId() : DEFAULT_SESSION_ID;
  }

  @Override
  public PartitionId partitionId() {
    return partitionId;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType type() {
    return primitiveType;
  }

  @Override
  public ThreadContext context() {
    return context;
  }

  @Override
  public PrimitiveState getState() {
    return state;
  }

  /**
   * Sets the session state.
   *
   * @param state the session state
   */
  private synchronized void onStateChange(PrimitiveState state) {
    if (this.state != state) {
      if (state == PrimitiveState.CLOSED) {
        if (connected) {
          onStateChange(PrimitiveState.SUSPENDED);
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
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    stateChangeListeners.add(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    stateChangeListeners.remove(listener);
  }

  /**
   * Recovers the client.
   */
  private void recover() {
    proxy = null;
    openProxy();
  }

  /**
   * Opens a new client.
   *
   * @return a future to be completed once the client has been opened
   */
  private CompletableFuture<SessionClient> openProxy() {
    if (connected) {
      log.debug("Opening proxy session");

      clientFuture = new OrderedFuture<>();
      openProxy(clientFuture);

      return clientFuture.thenApply(proxy -> {
        synchronized (this) {
          this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(SessionClient.class)
              .addValue(proxy.sessionId())
              .add("type", proxy.type())
              .add("name", proxy.name())
              .build());
          this.proxy = proxy;
          proxy.addStateChangeListener(this::onStateChange);
          eventListeners.forEach(proxy::addEventListener);
          onStateChange(PrimitiveState.CONNECTED);
        }
        return proxy;
      });
    }
    return Futures.exceptionalFuture(new IllegalStateException("Client not open"));
  }

  /**
   * Opens a new client, completing the provided future only once the client has been opened.
   *
   * @param future the future to be completed once the client is opened
   */
  private void openProxy(CompletableFuture<SessionClient> future) {
    proxyFactory.get().connect().whenComplete((proxy, error) -> {
      if (error == null) {
        future.complete(proxy);
      } else {
        recoverTask = context.schedule(Duration.ofSeconds(1), () -> openProxy(future));
      }
    });
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    SessionClient proxy = this.proxy;
    if (proxy != null) {
      return proxy.execute(operation);
    } else {
      return clientFuture.thenCompose(c -> c.execute(operation));
    }
  }

  @Override
  public synchronized void addEventListener(EventType eventType, Consumer<PrimitiveEvent> consumer) {
    eventListeners.put(eventType.canonicalize(), consumer);
    SessionClient proxy = this.proxy;
    if (proxy != null) {
      proxy.addEventListener(eventType, consumer);
    }
  }

  @Override
  public synchronized void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> consumer) {
    eventListeners.remove(eventType.canonicalize(), consumer);
    SessionClient proxy = this.proxy;
    if (proxy != null) {
      proxy.removeEventListener(eventType, consumer);
    }
  }

  @Override
  public synchronized CompletableFuture<SessionClient> connect() {
    if (!connected) {
      connected = true;
      return openProxy().thenApply(c -> this);
    }
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (connected) {
      connected = false;
      if (recoverTask != null) {
        recoverTask.cancel();
      }

      SessionClient proxy = this.proxy;
      if (proxy != null) {
        return proxy.close();
      } else {
        return clientFuture.thenCompose(c -> c.close());
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", proxy.name())
        .add("serviceType", proxy.type())
        .add("state", state)
        .toString();
  }
}