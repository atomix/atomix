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
package io.atomix.rest.utils;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Event session.
 */
class EventSession<T> {
  private final Queue<T> events = new ConcurrentLinkedQueue<>();
  private final Queue<CompletableFuture<T>> futures = new ConcurrentLinkedQueue<>();

  /**
   * Adds an event to the session.
   *
   * @param event the event to add
   */
  void addEvent(T event) {
    CompletableFuture<T> future = futures.poll();
    if (future != null) {
      future.complete(event);
    } else {
      events.add(event);
      if (events.size() > 100) {
        events.remove();
      }
    }
  }

  /**
   * Completes the given response with the next event.
   *
   * @return a future to be completed with the next event
   */
  CompletableFuture<T> nextEvent() {
    T event = events.poll();
    if (event != null) {
      return CompletableFuture.completedFuture(event);
    } else {
      CompletableFuture<T> future = new CompletableFuture<>();
      futures.add(future);
      return future;
    }
  }

  /**
   * Closes the session.
   */
  void close() {
    futures.forEach(future -> future.completeExceptionally(new IllegalStateException("Closed session")));
  }
}