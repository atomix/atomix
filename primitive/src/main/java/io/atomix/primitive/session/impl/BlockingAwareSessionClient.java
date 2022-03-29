// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.session.impl;

import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.session.SessionClient;
import io.atomix.utils.concurrent.ThreadContext;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.atomix.utils.concurrent.Futures.asyncFuture;
import static io.atomix.utils.concurrent.Futures.orderedFuture;

/**
 * Session client delegate that completes futures on a thread pool.
 */
public class BlockingAwareSessionClient extends DelegatingSessionClient {
  private final ThreadContext context;
  private final Map<Consumer<PrimitiveState>, Consumer<PrimitiveState>> stateChangeListeners = Maps.newConcurrentMap();
  private final Map<Consumer<PrimitiveEvent>, Consumer<PrimitiveEvent>> eventListeners = Maps.newConcurrentMap();
  private volatile CompletableFuture<SessionClient> connectFuture;
  private volatile CompletableFuture<Void> closeFuture;

  public BlockingAwareSessionClient(SessionClient session, ThreadContext context) {
    super(session);
    this.context = context;
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    Consumer<PrimitiveState> wrappedListener = state -> context.execute(() -> listener.accept(state));
    stateChangeListeners.put(listener, wrappedListener);
    super.addStateChangeListener(wrappedListener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    Consumer<PrimitiveState> wrappedListener = stateChangeListeners.remove(listener);
    if (wrappedListener != null) {
      super.removeStateChangeListener(wrappedListener);
    }
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    return asyncFuture(super.execute(operation), context);
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    Consumer<PrimitiveEvent> wrappedListener = e -> context.execute(() -> listener.accept(e));
    eventListeners.put(listener, wrappedListener);
    super.addEventListener(eventType, wrappedListener);
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    Consumer<PrimitiveEvent> wrappedListener = eventListeners.remove(listener);
    if (wrappedListener != null) {
      super.removeEventListener(eventType, wrappedListener);
    }
  }

  @Override
  public CompletableFuture<SessionClient> connect() {
    if (connectFuture == null) {
      synchronized (this) {
        if (connectFuture == null) {
          connectFuture = orderedFuture(asyncFuture(super.connect(), context));
        }
      }
    }
    return connectFuture;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = orderedFuture(asyncFuture(super.close(), context));
        }
      }
    }
    return closeFuture;
  }

  @Override
  public CompletableFuture<Void> delete() {
    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = orderedFuture(asyncFuture(super.delete(), context));
        }
      }
    }
    return closeFuture;
  }
}
