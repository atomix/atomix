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
package io.atomix.cluster.messaging.impl;

import com.google.common.collect.Sets;
import io.atomix.messaging.BroadcastService;
import io.atomix.messaging.ManagedBroadcastService;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Test broadcast service.
 */
public class TestBroadcastService implements ManagedBroadcastService {
  private final Set<TestBroadcastService> services;
  private final Set<Consumer<byte[]>> listeners = Sets.newCopyOnWriteArraySet();
  private final AtomicBoolean started = new AtomicBoolean();

  public TestBroadcastService(Set<TestBroadcastService> services) {
    this.services = services;
  }

  @Override
  public void broadcast(byte[] message) {
    services.forEach(service -> {
      service.listeners.forEach(listener -> {
        listener.accept(message);
      });
    });
  }

  @Override
  public void addListener(Consumer<byte[]> listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(Consumer<byte[]> listener) {
    listeners.remove(listener);
  }

  @Override
  public CompletableFuture<BroadcastService> start() {
    services.add(this);
    started.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    services.remove(this);
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
