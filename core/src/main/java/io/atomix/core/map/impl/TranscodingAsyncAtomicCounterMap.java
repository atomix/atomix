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
package io.atomix.core.map.impl;

import io.atomix.core.map.AsyncAtomicCounterMap;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An {@code AsyncAtomicCounterMap} that transcodes keys.
 */
public class TranscodingAsyncAtomicCounterMap<K1, K2> extends DelegatingAsyncPrimitive implements AsyncAtomicCounterMap<K1> {
  private final AsyncAtomicCounterMap<K2> backingMap;
  private final Function<K1, K2> keyEncoder;
  private final Function<K2, K1> keyDecoder;

  public TranscodingAsyncAtomicCounterMap(
      AsyncAtomicCounterMap<K2> backingMap,
      Function<K1, K2> keyEncoder,
      Function<K2, K1> keyDecoder) {
    super(backingMap);
    this.backingMap = backingMap;
    this.keyEncoder = k -> k == null ? null : keyEncoder.apply(k);
    this.keyDecoder = k -> k == null ? null : keyDecoder.apply(k);
  }

  @Override
  public CompletableFuture<Long> incrementAndGet(K1 key) {
    try {
      return backingMap.incrementAndGet(keyEncoder.apply(key));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Long> decrementAndGet(K1 key) {
    try {
      return backingMap.decrementAndGet(keyEncoder.apply(key));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Long> getAndIncrement(K1 key) {
    try {
      return backingMap.getAndIncrement(keyEncoder.apply(key));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Long> getAndDecrement(K1 key) {
    try {
      return backingMap.getAndDecrement(keyEncoder.apply(key));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Long> addAndGet(K1 key, long delta) {
    try {
      return backingMap.addAndGet(keyEncoder.apply(key), delta);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Long> getAndAdd(K1 key, long delta) {
    try {
      return backingMap.getAndAdd(keyEncoder.apply(key), delta);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Long> get(K1 key) {
    try {
      return backingMap.get(keyEncoder.apply(key));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Long> put(K1 key, long newValue) {
    try {
      return backingMap.put(keyEncoder.apply(key), newValue);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Long> putIfAbsent(K1 key, long newValue) {
    try {
      return backingMap.putIfAbsent(keyEncoder.apply(key), newValue);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> replace(K1 key, long expectedOldValue, long newValue) {
    try {
      return backingMap.replace(keyEncoder.apply(key), expectedOldValue, newValue);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Long> remove(K1 key) {
    try {
      return backingMap.remove(keyEncoder.apply(key));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> remove(K1 key, long value) {
    try {
      return backingMap.remove(keyEncoder.apply(key), value);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Integer> size() {
    try {
      return backingMap.size();
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    try {
      return backingMap.isEmpty();
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> clear() {
    try {
      return backingMap.clear();
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public AtomicCounterMap<K1> sync(Duration operationTimeout) {
    return new BlockingAtomicCounterMap<>(this, operationTimeout.toMillis());
  }
}
