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
package io.atomix.primitive.proxy.impl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.primitive.session.SessionId;
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
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Primitive proxy that supports recovery.
 */
public class RecoveringPartitionProxy implements PartitionProxy {
  private static final SessionId DEFAULT_SESSION_ID = SessionId.from(0);
  private final PartitionId partitionId;
  private final String name;
  private final PrimitiveType primitiveType;
  private final Supplier<PartitionProxy> proxyFactory;
  private final Scheduler scheduler;
  private Logger log;
  private volatile OrderedFuture<PartitionProxy> clientFuture;
  private volatile PartitionProxy proxy;
  private volatile State state = State.CLOSED;
  private final Set<Consumer<PartitionProxy.State>> stateChangeListeners = Sets.newCopyOnWriteArraySet();
  private final Multimap<EventType, Consumer<PrimitiveEvent>> eventListeners = HashMultimap.create();
  private Scheduled recoverTask;
  private volatile boolean connected = false;

  public RecoveringPartitionProxy(String clientId, PartitionId partitionId, String name, PrimitiveType primitiveType, Supplier<PartitionProxy> proxyFactory, Scheduler scheduler) {
    this.partitionId = checkNotNull(partitionId);
    this.name = checkNotNull(name);
    this.primitiveType = checkNotNull(primitiveType);
    this.proxyFactory = checkNotNull(proxyFactory);
    this.scheduler = checkNotNull(scheduler);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PartitionProxy.class)
        .addValue(clientId)
        .build());
  }

  @Override
  public SessionId sessionId() {
    PartitionProxy proxy = this.proxy;
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
  public PartitionProxy.State getState() {
    return state;
  }

  /**
   * Sets the session state.
   *
   * @param state the session state
   */
  private synchronized void onStateChange(PartitionProxy.State state) {
    if (this.state != state) {
      if (state == PartitionProxy.State.CLOSED) {
        if (connected) {
          onStateChange(PartitionProxy.State.SUSPENDED);
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
  public void addStateChangeListener(Consumer<PartitionProxy.State> listener) {
    stateChangeListeners.add(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PartitionProxy.State> listener) {
    stateChangeListeners.remove(listener);
  }

  /**
   * Verifies that the client is open.
   */
  private void checkOpen() {
    checkState(connected, "client not open");
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
  private CompletableFuture<PartitionProxy> openProxy() {
    if (connected) {
      log.debug("Opening proxy session");

      clientFuture = new OrderedFuture<>();
      openProxy(clientFuture);

      return clientFuture.thenApply(proxy -> {
        synchronized (this) {
          this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PartitionProxy.class)
              .addValue(proxy.sessionId())
              .add("type", proxy.type())
              .add("name", proxy.name())
              .build());
          this.proxy = proxy;
          proxy.addStateChangeListener(this::onStateChange);
          eventListeners.forEach(proxy::addEventListener);
          onStateChange(PartitionProxy.State.CONNECTED);
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
  private void openProxy(CompletableFuture<PartitionProxy> future) {
    proxyFactory.get().connect().whenComplete((proxy, error) -> {
      if (error == null) {
        future.complete(proxy);
      } else {
        recoverTask = scheduler.schedule(Duration.ofSeconds(1), () -> openProxy(future));
      }
    });
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    checkOpen();
    PartitionProxy proxy = this.proxy;
    if (proxy != null) {
      return proxy.execute(operation);
    } else {
      return clientFuture.thenCompose(c -> c.execute(operation));
    }
  }

  @Override
  public synchronized void addEventListener(EventType eventType, Consumer<PrimitiveEvent> consumer) {
    checkOpen();
    eventListeners.put(eventType.canonicalize(), consumer);
    PartitionProxy proxy = this.proxy;
    if (proxy != null) {
      proxy.addEventListener(eventType, consumer);
    }
  }

  @Override
  public synchronized void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> consumer) {
    checkOpen();
    eventListeners.remove(eventType.canonicalize(), consumer);
    PartitionProxy proxy = this.proxy;
    if (proxy != null) {
      proxy.removeEventListener(eventType, consumer);
    }
  }

  @Override
  public synchronized CompletableFuture<PartitionProxy> connect() {
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

      PartitionProxy proxy = this.proxy;
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