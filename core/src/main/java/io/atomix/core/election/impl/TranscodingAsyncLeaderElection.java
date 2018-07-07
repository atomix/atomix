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
import io.atomix.core.election.AsyncLeaderElection;
import io.atomix.core.election.LeaderElection;
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
 * Transcoding document tree.
 */
public class TranscodingAsyncLeaderElection<V1, V2> extends DelegatingAsyncPrimitive implements AsyncLeaderElection<V1> {

  private final AsyncLeaderElection<V2> backingElection;
  private final Function<V1, V2> valueEncoder;
  private final Function<V2, V1> valueDecoder;
  private final Map<LeadershipEventListener<V1>, InternalLeadershipEventListener> listeners = Maps.newIdentityHashMap();

  public TranscodingAsyncLeaderElection(AsyncLeaderElection<V2> backingElection, Function<V1, V2> valueEncoder, Function<V2, V1> valueDecoder) {
    super(backingElection);
    this.backingElection = backingElection;
    this.valueEncoder = valueEncoder;
    this.valueDecoder = valueDecoder;
  }

  @Override
  public CompletableFuture<Leadership<V1>> run(V1 identifier) {
    return backingElection.run(valueEncoder.apply(identifier)).thenApply(l -> l.map(valueDecoder));
  }

  @Override
  public CompletableFuture<Void> withdraw(V1 identifier) {
    return backingElection.withdraw(valueEncoder.apply(identifier));
  }

  @Override
  public CompletableFuture<Boolean> anoint(V1 identifier) {
    return backingElection.anoint(valueEncoder.apply(identifier));
  }

  @Override
  public CompletableFuture<Void> evict(V1 identifier) {
    return backingElection.evict(valueEncoder.apply(identifier));
  }

  @Override
  public CompletableFuture<Boolean> promote(V1 identifier) {
    return backingElection.promote(valueEncoder.apply(identifier));
  }

  @Override
  public CompletableFuture<Leadership<V1>> getLeadership() {
    return backingElection.getLeadership().thenApply(l -> l.map(valueDecoder));
  }

  @Override
  public CompletableFuture<Void> addListener(LeadershipEventListener<V1> listener) {
    synchronized (listeners) {
      InternalLeadershipEventListener internalListener =
          listeners.computeIfAbsent(listener, k -> new InternalLeadershipEventListener(listener));
      return backingElection.addListener(internalListener);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(LeadershipEventListener<V1> listener) {
    synchronized (listeners) {
      InternalLeadershipEventListener internalListener = listeners.remove(listener);
      if (internalListener != null) {
        return backingElection.removeListener(internalListener);
      } else {
        return CompletableFuture.completedFuture(null);
      }
    }
  }

  @Override
  public LeaderElection<V1> sync(Duration operationTimeout) {
    return new BlockingLeaderElection<>(this, operationTimeout.toMillis());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("backingElection", backingElection)
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
