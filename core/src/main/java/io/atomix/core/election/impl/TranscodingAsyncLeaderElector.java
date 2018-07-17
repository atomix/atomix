/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.election.impl;

import com.google.common.collect.Maps;
import io.atomix.core.election.AsyncLeaderElector;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Transcoding leader elector.
 */
public class TranscodingAsyncLeaderElector<V1, V2> extends DelegatingAsyncPrimitive implements AsyncLeaderElector<V1> {

  private final AsyncLeaderElector<V2> backingElector;
  private final Function<V1, V2> valueEncoder;
  private final Function<V2, V1> valueDecoder;
  private final Map<LeadershipEventListener<V1>, InternalLeadershipEventListener> listeners = Maps.newIdentityHashMap();

  public TranscodingAsyncLeaderElector(AsyncLeaderElector<V2> backingElector, Function<V1, V2> valueEncoder, Function<V2, V1> valueDecoder) {
    super(backingElector);
    this.backingElector = backingElector;
    this.valueEncoder = valueEncoder;
    this.valueDecoder = valueDecoder;
  }

  @Override
  public CompletableFuture<Leadership<V1>> run(String topic, V1 identifier) {
    return backingElector.run(topic, valueEncoder.apply(identifier))
        .thenApply(leadership -> leadership.map(valueDecoder));
  }

  @Override
  public CompletableFuture<Void> withdraw(String topic, V1 identifier) {
    return backingElector.withdraw(topic, valueEncoder.apply(identifier));
  }

  @Override
  public CompletableFuture<Boolean> anoint(String topic, V1 identifier) {
    return backingElector.anoint(topic, valueEncoder.apply(identifier));
  }

  @Override
  public CompletableFuture<Void> evict(V1 identifier) {
    return backingElector.evict(valueEncoder.apply(identifier));
  }

  @Override
  public CompletableFuture<Boolean> promote(String topic, V1 identifier) {
    return backingElector.promote(topic, valueEncoder.apply(identifier));
  }

  @Override
  public CompletableFuture<Leadership<V1>> getLeadership(String topic) {
    return backingElector.getLeadership(topic)
        .thenApply(leadership -> leadership.map(valueDecoder));
  }

  @Override
  public CompletableFuture<Map<String, Leadership<V1>>> getLeaderships() {
    return backingElector.getLeaderships()
        .thenApply(leaderships -> Maps.transformValues(leaderships, leadership -> leadership.map(valueDecoder)));
  }

  @Override
  public CompletableFuture<Void> addListener(LeadershipEventListener<V1> listener) {
    synchronized (listeners) {
      InternalLeadershipEventListener internalListener =
          listeners.computeIfAbsent(listener, k -> new InternalLeadershipEventListener(listener));
      return backingElector.addListener(internalListener);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(LeadershipEventListener<V1> listener) {
    synchronized (listeners) {
      InternalLeadershipEventListener internalListener = listeners.remove(listener);
      if (internalListener != null) {
        return backingElector.removeListener(internalListener);
      } else {
        return CompletableFuture.completedFuture(null);
      }
    }
  }

  @Override
  public CompletableFuture<Void> addListener(String topic, LeadershipEventListener<V1> listener) {
    synchronized (listeners) {
      InternalLeadershipEventListener internalListener =
          listeners.computeIfAbsent(listener, k -> new InternalLeadershipEventListener(listener));
      return backingElector.addListener(topic, internalListener);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(String topic, LeadershipEventListener<V1> listener) {
    synchronized (listeners) {
      InternalLeadershipEventListener internalListener = listeners.remove(listener);
      if (internalListener != null) {
        return backingElector.removeListener(topic, internalListener);
      } else {
        return CompletableFuture.completedFuture(null);
      }
    }
  }

  @Override
  public LeaderElector<V1> sync(Duration operationTimeout) {
    return new BlockingLeaderElector<>(this, operationTimeout.toMillis());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("backingElector", backingElector)
        .toString();
  }

  private class InternalLeadershipEventListener implements LeadershipEventListener<V2> {
    private final LeadershipEventListener<V1> listener;

    InternalLeadershipEventListener(LeadershipEventListener<V1> listener) {
      this.listener = listener;
    }

    @Override
    public void event(LeadershipEvent<V2> event) {
      listener.event(new LeadershipEvent<>(
          event.type(),
          event.topic(),
          event.oldLeadership() != null ? event.oldLeadership().map(valueDecoder) : null,
          event.newLeadership() != null ? event.newLeadership().map(valueDecoder) : null));
    }
  }
}
