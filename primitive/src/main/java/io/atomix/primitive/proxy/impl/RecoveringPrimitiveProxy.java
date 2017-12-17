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

import com.google.common.collect.Sets;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.proxy.PrimitiveProxy;
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
public class RecoveringPrimitiveProxy extends AbstractPrimitiveProxy {
  private static final SessionId DEFAULT_SESSION_ID = SessionId.from(0);
  private final String name;
  private final PrimitiveType primitiveType;
  private final Supplier<PrimitiveProxy> proxyFactory;
  private final Scheduler scheduler;
  private Logger log;
  private volatile OrderedFuture<PrimitiveProxy> clientFuture;
  private volatile PrimitiveProxy proxy;
  private volatile PrimitiveProxy.State state = PrimitiveProxy.State.SUSPENDED;
  private final Set<Consumer<PrimitiveProxy.State>> stateChangeListeners = Sets.newCopyOnWriteArraySet();
  private final Set<Consumer<PrimitiveEvent>> eventListeners = Sets.newCopyOnWriteArraySet();
  private Scheduled recoverTask;
  private volatile boolean connected = false;

  public RecoveringPrimitiveProxy(String clientId, String name, PrimitiveType primitiveType, Supplier<PrimitiveProxy> proxyFactory, Scheduler scheduler) {
    this.name = checkNotNull(name);
    this.primitiveType = checkNotNull(primitiveType);
    this.proxyFactory = checkNotNull(proxyFactory);
    this.scheduler = checkNotNull(scheduler);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveProxy.class)
        .addValue(clientId)
        .build());
  }

  @Override
  public SessionId sessionId() {
    PrimitiveProxy proxy = this.proxy;
    return proxy != null ? proxy.sessionId() : DEFAULT_SESSION_ID;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType serviceType() {
    return primitiveType;
  }

  @Override
  public PrimitiveProxy.State getState() {
    return state;
  }

  /**
   * Sets the session state.
   *
   * @param state the session state
   */
  private synchronized void onStateChange(PrimitiveProxy.State state) {
    if (this.state != state) {
      if (state == PrimitiveProxy.State.CLOSED) {
        if (connected) {
          onStateChange(PrimitiveProxy.State.SUSPENDED);
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
  public void addStateChangeListener(Consumer<PrimitiveProxy.State> listener) {
    stateChangeListeners.add(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveProxy.State> listener) {
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
  private CompletableFuture<PrimitiveProxy> openProxy() {
    if (connected) {
      log.debug("Opening proxy session");

      clientFuture = new OrderedFuture<>();
      openProxy(clientFuture);

      return clientFuture.thenApply(client -> {
        synchronized (this) {
          this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveProxy.class)
              .addValue(client.sessionId())
              .add("type", client.serviceType())
              .add("name", client.name())
              .build());
          this.proxy = client;
          client.addStateChangeListener(this::onStateChange);
          eventListeners.forEach(client::addEventListener);
          onStateChange(PrimitiveProxy.State.CONNECTED);
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
  private void openProxy(CompletableFuture<PrimitiveProxy> future) {
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
    PrimitiveProxy proxy = this.proxy;
    if (proxy != null) {
      return proxy.execute(operation);
    } else {
      return clientFuture.thenCompose(c -> c.execute(operation));
    }
  }

  @Override
  public synchronized void addEventListener(Consumer<PrimitiveEvent> consumer) {
    checkOpen();
    eventListeners.add(consumer);
    PrimitiveProxy proxy = this.proxy;
    if (proxy != null) {
      proxy.addEventListener(consumer);
    }
  }

  @Override
  public synchronized void removeEventListener(Consumer<PrimitiveEvent> consumer) {
    checkOpen();
    eventListeners.remove(consumer);
    PrimitiveProxy proxy = this.proxy;
    if (proxy != null) {
      proxy.removeEventListener(consumer);
    }
  }

  @Override
  public synchronized CompletableFuture<PrimitiveProxy> connect() {
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

      PrimitiveProxy proxy = this.proxy;
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
        .add("serviceType", proxy.serviceType())
        .add("state", state)
        .toString();
  }
}