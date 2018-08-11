/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.value.impl;

import com.google.common.collect.Maps;
import io.atomix.core.value.AsyncDistributedValue;
import io.atomix.core.value.DistributedValue;
import io.atomix.core.value.DistributedValueType;
import io.atomix.core.value.ValueEvent;
import io.atomix.core.value.ValueEventListener;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.value.ValueDelegate;
import io.atomix.primitive.protocol.value.ValueDelegateEventListener;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Gossip-based distributed value.
 */
public class GossipDistributedValue<V> implements AsyncDistributedValue<V> {
  private final String name;
  private final PrimitiveProtocol protocol;
  private final ValueDelegate<V> value;
  private final Map<ValueEventListener<V>, ValueDelegateEventListener<V>> listenerMap = Maps.newConcurrentMap();

  public GossipDistributedValue(String name, PrimitiveProtocol protocol, ValueDelegate<V> value) {
    this.name = name;
    this.protocol = protocol;
    this.value = value;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType type() {
    return DistributedValueType.instance();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return protocol;
  }

  @Override
  public CompletableFuture<V> get() {
    try {
      return CompletableFuture.completedFuture(value.get());
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<V> getAndSet(V value) {
    try {
      return CompletableFuture.completedFuture(this.value.getAndSet(value));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> set(V value) {
    try {
      this.value.set(value);
      return CompletableFuture.completedFuture(null);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> addListener(ValueEventListener<V> listener) {
    ValueDelegateEventListener<V> eventListener = event -> listener.event(new ValueEvent<>(ValueEvent.Type.UPDATE, event.value(), null));
    if (listenerMap.putIfAbsent(listener, eventListener) == null) {
      value.addListener(eventListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(ValueEventListener<V> listener) {
    ValueDelegateEventListener<V> eventListener = listenerMap.remove(listener);
    if (eventListener != null) {
      value.removeListener(eventListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    try {
      value.close();
      return CompletableFuture.completedFuture(null);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> delete() {
    try {
      value.set(null);
      value.close();
      return CompletableFuture.completedFuture(null);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public DistributedValue<V> sync(Duration operationTimeout) {
    return new BlockingDistributedValue<>(this, operationTimeout.toMillis());
  }
}
