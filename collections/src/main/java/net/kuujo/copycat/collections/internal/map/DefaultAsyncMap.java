/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.collections.internal.map;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.collections.AsyncMap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Default asynchronous map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncMap<K, V> implements AsyncMap<K, V> {
  private final StateMachine<AsyncMapState<K, V>> stateMachine;
  private AsyncMapProxy<K, V> proxy;

  public DefaultAsyncMap(StateMachine<AsyncMapState<K, V>> stateMachine) {
    this.stateMachine = stateMachine;
  }

  @Override
  public String name() {
    return stateMachine.name();
  }

  @Override
  public Cluster cluster() {
    return stateMachine.cluster();
  }

  @Override
  public CopycatState state() {
    return stateMachine.state();
  }

  @Override
  public CompletableFuture<V> put(K key, V value) {
    if (proxy == null) {
      CompletableFuture<V> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Map closed"));
      return future;
    }
    return proxy.put(key, value);
  }

  @Override
  public CompletableFuture<V> get(K key) {
    if (proxy == null) {
      CompletableFuture<V> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Map closed"));
      return future;
    }
    return proxy.get(key);
  }

  @Override
  public CompletableFuture<V> remove(K key) {
    if (proxy == null) {
      CompletableFuture<V> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Map closed"));
      return future;
    }
    return proxy.remove(key);
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    if (proxy == null) {
      CompletableFuture<Boolean> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Map closed"));
      return future;
    }
    return proxy.containsKey(key);
  }

  @Override
  public CompletableFuture<Set<K>> keySet() {
    if (proxy == null) {
      CompletableFuture<Set<K>> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Map closed"));
      return future;
    }
    return proxy.keySet();
  }

  @Override
  public CompletableFuture<Set<Map.Entry<K, V>>> entrySet() {
    if (proxy == null) {
      CompletableFuture<Set<Map.Entry<K, V>>> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Map closed"));
      return future;
    }
    return proxy.entrySet();
  }

  @Override
  public CompletableFuture<Collection<V>> values() {
    if (proxy == null) {
      CompletableFuture<Collection<V>> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Map closed"));
      return future;
    }
    return proxy.values();
  }

  @Override
  public CompletableFuture<Integer> size() {
    if (proxy == null) {
      CompletableFuture<Integer> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Map closed"));
      return future;
    }
    return proxy.size();
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    if (proxy == null) {
      CompletableFuture<Boolean> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Map closed"));
      return future;
    }
    return proxy.isEmpty();
  }

  @Override
  public CompletableFuture<Void> clear() {
    if (proxy == null) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Map closed"));
      return future;
    }
    return proxy.clear();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return stateMachine.delete();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> open() {
    return stateMachine.open().thenRun(() -> {
      this.proxy = stateMachine.createProxy(AsyncMapProxy.class);
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}
