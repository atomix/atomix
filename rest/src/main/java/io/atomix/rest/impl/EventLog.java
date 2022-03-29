// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.impl;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Session registry.
 */
public class EventLog<L, E> {
  private final L listener;
  private final AtomicBoolean open = new AtomicBoolean();
  private final Queue<E> events = new ConcurrentLinkedQueue<>();
  private final Queue<CompletableFuture<E>> futures = new ConcurrentLinkedQueue<>();

  public EventLog(Function<EventLog<L, E>, L> listenerFactory) {
    this.listener = listenerFactory.apply(this);
  }

  /**
   * Returns a boolean indicating whether the event consumer needs to be registered.
   *
   * @return indicates whether the event consumer needs to be registered
   */
  public boolean open() {
    return open.compareAndSet(false, true);
  }

  /**
   * Returns the event listener.
   *
   * @return the event listener
   */
  public L listener() {
    return listener;
  }

  /**
   * Completes the given response with the next event.
   *
   * @return a future to be completed with the next event
   */
  public CompletableFuture<E> nextEvent() {
    E event = events.poll();
    if (event != null) {
      return CompletableFuture.completedFuture(event);
    } else {
      CompletableFuture<E> future = new CompletableFuture<>();
      futures.add(future);
      return future;
    }
  }

  /**
   * Adds an event to the log.
   *
   * @param event the event to add
   */
  public void addEvent(E event) {
    CompletableFuture<E> future = futures.poll();
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
   * Closes the session.
   */
  public boolean close() {
    if (open.compareAndSet(true, false)) {
      futures.forEach(future -> future.completeExceptionally(new IllegalStateException("Closed session")));
      return true;
    }
    return false;
  }
}
