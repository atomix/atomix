// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value.impl;

import com.google.common.collect.Sets;
import io.atomix.core.value.AsyncAtomicValue;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueEvent;
import io.atomix.core.value.AtomicValueEventListener;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Atomix counter implementation.
 */
public class AtomicValueProxy extends AbstractAsyncPrimitive<AsyncAtomicValue<byte[]>, AtomicValueService> implements AsyncAtomicValue<byte[]>, AtomicValueClient {
  private final Set<AtomicValueEventListener<byte[]>> eventListeners = Sets.newConcurrentHashSet();

  public AtomicValueProxy(ProxyClient<AtomicValueService> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public void change(byte[] newValue, byte[] oldValue) {
    eventListeners.forEach(l -> l.event(new AtomicValueEvent<>(AtomicValueEvent.Type.UPDATE, newValue, oldValue)));
  }

  @Override
  public CompletableFuture<byte[]> get() {
    return getProxyClient().applyBy(name(), service -> service.get());
  }

  @Override
  public CompletableFuture<Void> set(byte[] value) {
    return getProxyClient().acceptBy(name(), service -> service.set(value));
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(byte[] expect, byte[] update) {
    return getProxyClient().applyBy(name(), service -> service.compareAndSet(expect, update));
  }

  @Override
  public CompletableFuture<byte[]> getAndSet(byte[] value) {
    return getProxyClient().applyBy(name(), service -> service.getAndSet(value));
  }

  @Override
  public CompletableFuture<Void> addListener(AtomicValueEventListener<byte[]> listener) {
    if (eventListeners.isEmpty()) {
      return getProxyClient().acceptBy(name(), service -> service.addListener()).thenRun(() -> eventListeners.add(listener));
    } else {
      eventListeners.add(listener);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(AtomicValueEventListener<byte[]> listener) {
    if (eventListeners.remove(listener) && eventListeners.isEmpty()) {
      return getProxyClient().acceptBy(name(), service -> service.removeListener()).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<AsyncAtomicValue<byte[]>> connect() {
    return super.connect()
        .thenCompose(v -> getProxyClient().getPartition(name()).connect())
        .thenApply(v -> this);
  }

  @Override
  public AtomicValue<byte[]> sync(Duration operationTimeout) {
    return new BlockingAtomicValue<>(this, operationTimeout.toMillis());
  }
}
