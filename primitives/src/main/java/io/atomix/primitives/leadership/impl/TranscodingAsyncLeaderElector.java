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
package io.atomix.primitives.leadership.impl;

import com.google.common.collect.Maps;
import io.atomix.primitives.leadership.AsyncLeaderElector;
import io.atomix.primitives.leadership.Leadership;
import io.atomix.primitives.leadership.LeadershipEvent;
import io.atomix.primitives.leadership.LeadershipEventListener;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Transcoding document tree.
 */
public class TranscodingAsyncLeaderElector<V1, V2> implements AsyncLeaderElector<V1> {

  private final AsyncLeaderElector<V2> backingElector;
  private final Function<V1, V2> valueEncoder;
  private final Function<V2, V1> valueDecoder;
  private final Map<LeadershipEventListener<V1>, InternalLeadershipEventListener> listeners = Maps.newIdentityHashMap();

  public TranscodingAsyncLeaderElector(AsyncLeaderElector<V2> backingElector, Function<V1, V2> valueEncoder, Function<V2, V1> valueDecoder) {
    this.backingElector = backingElector;
    this.valueEncoder = valueEncoder;
    this.valueDecoder = valueDecoder;
  }

  @Override
  public String name() {
    return backingElector.name();
  }

  @Override
  public CompletableFuture<Leadership<V1>> run(V1 identifier) {
    return backingElector.run(valueEncoder.apply(identifier)).thenApply(l -> l.map(valueDecoder));
  }

  @Override
  public CompletableFuture<Void> withdraw() {
    return backingElector.withdraw();
  }

  @Override
  public CompletableFuture<Boolean> anoint(V1 identifier) {
    return backingElector.anoint(valueEncoder.apply(identifier));
  }

  @Override
  public CompletableFuture<Void> evict(V1 identifier) {
    return backingElector.evict(valueEncoder.apply(identifier));
  }

  @Override
  public CompletableFuture<Boolean> promote(V1 identifier) {
    return backingElector.promote(valueEncoder.apply(identifier));
  }

  @Override
  public CompletableFuture<Leadership<V1>> getLeadership() {
    return backingElector.getLeadership().thenApply(l -> l.map(valueDecoder));
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
  public CompletableFuture<Void> close() {
    return backingElector.close();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("backingTree", backingElector)
        .toString();
  }

  private class InternalLeadershipEventListener implements LeadershipEventListener<V2> {
    private final LeadershipEventListener<V1> listener;

    InternalLeadershipEventListener(LeadershipEventListener<V1> listener) {
      this.listener = listener;
    }

    @Override
    public void onEvent(LeadershipEvent<V2> event) {
      listener.onEvent(new LeadershipEvent<>(
          event.type(),
          event.oldLeadership().map(valueDecoder),
          event.newLeadership().map(valueDecoder)));
    }
  }
}
