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
package io.atomix.protocols.gossip.value;

import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.Subscription;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.value.ValueDelegate;
import io.atomix.primitive.protocol.value.ValueDelegateEvent;
import io.atomix.primitive.protocol.value.ValueDelegateEventListener;
import io.atomix.protocols.gossip.CrdtProtocolConfig;
import io.atomix.protocols.gossip.TimestampProvider;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Last-write wins value.
 */
public class CrdtValueDelegate<V> implements ValueDelegate<V> {
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .register(Value.class)
      .build());

  private final ClusterEventService clusterEventService;
  private final ScheduledExecutorService executorService;
  private final Serializer valueSerializer;
  private final TimestampProvider<V> timestampProvider;
  private final String subject;
  private volatile CompletableFuture<Subscription> subscribeFuture;
  private volatile ScheduledFuture<?> broadcastFuture;
  private final AtomicReference<Value> currentValue = new AtomicReference<>();
  private final Set<ValueDelegateEventListener<V>> eventListeners = Sets.newCopyOnWriteArraySet();

  public CrdtValueDelegate(String name, Serializer serializer, CrdtProtocolConfig config, PrimitiveManagementService managementService) {
    this.clusterEventService = managementService.getEventService();
    this.executorService = managementService.getExecutorService();
    this.valueSerializer = serializer;
    this.timestampProvider = config.getTimestampProvider();
    this.subject = String.format("atomix-crdt-value-%s", name);
    subscribeFuture = clusterEventService.subscribe(subject, SERIALIZER::decode, this::updateValue, executorService);
    broadcastFuture = executorService.scheduleAtFixedRate(
        this::broadcastValue, config.getGossipInterval().toMillis(), config.getGossipInterval().toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public V get() {
    Value value = currentValue.get();
    return value != null ? decode(value.value()) : null;
  }

  @Override
  public V getAndSet(V value) {
    Value newValue = new Value(encode(value), timestampProvider.get(value));
    while (true) {
      Value oldValue = currentValue.get();
      if (newValue.isNewerThan(oldValue)) {
        if (currentValue.compareAndSet(oldValue, newValue)) {
          if (oldValue == null || !Objects.equals(oldValue.value(), newValue.value())) {
            eventListeners.forEach(listener -> listener.event(new ValueDelegateEvent<>(ValueDelegateEvent.Type.UPDATE, value)));
          }
          return oldValue != null ? decode(oldValue.value()) : null;
        }
      } else {
        return value;
      }
    }
  }

  @Override
  public void set(V value) {
    Value newValue = new Value(encode(value), timestampProvider.get(value));
    while (true) {
      Value oldValue = currentValue.get();
      if (newValue.isNewerThan(oldValue)) {
        if (currentValue.compareAndSet(oldValue, newValue)) {
          if (oldValue == null || !Objects.equals(oldValue.value(), newValue.value())) {
            eventListeners.forEach(listener -> listener.event(new ValueDelegateEvent<>(ValueDelegateEvent.Type.UPDATE, value)));
          }
          break;
        }
      } else {
        break;
      }
    }
  }

  @Override
  public void addListener(ValueDelegateEventListener<V> listener) {
    eventListeners.add(listener);
  }

  @Override
  public void removeListener(ValueDelegateEventListener<V> listener) {
    eventListeners.remove(listener);
  }

  /**
   * Encodes the given value to a string for internal storage.
   *
   * @param value the value to encode
   * @return the encoded value
   */
  private String encode(Object value) {
    return BaseEncoding.base16().encode(valueSerializer.encode(value));
  }

  /**
   * Decodes the given value from a string.
   *
   * @param value the value to decode
   * @return the decoded value
   */
  protected V decode(String value) {
    return valueSerializer.decode(BaseEncoding.base16().decode(value));
  }

  /**
   * Updates the value.
   *
   * @param value the value
   */
  private void updateValue(Value value) {
    while (true) {
      Value current = currentValue.get();
      if (value.isNewerThan(current)) {
        if (currentValue.compareAndSet(current, value)) {
          if (current == null || !Objects.equals(current.value(), value.value())) {
            eventListeners.forEach(listener -> listener.event(new ValueDelegateEvent<>(ValueDelegateEvent.Type.UPDATE, decode(value.value()))));
          }
          break;
        }
      } else {
        break;
      }
    }
  }

  /**
   * Broadcasts the value to all peers.
   */
  private void broadcastValue() {
    clusterEventService.broadcast(subject, currentValue.get(), SERIALIZER::encode);
  }

  @Override
  public void close() {
    broadcastFuture.cancel(false);
    subscribeFuture.thenAccept(subscription -> subscription.close());
  }
}
