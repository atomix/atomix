// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.impl;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Rest event manager.
 */
public class EventManager {
  private final Map<Class<?>, Map<String, EventLog>> eventRegistries = new ConcurrentHashMap<>();

  /**
   * Returns an event log if it already exists.
   *
   * @param type the log type
   * @param name the log name
   * @param <L> the listener type
   * @param <E> the event type
   * @return the event log
   */
  @SuppressWarnings("unchecked")
  public <L, E> EventLog<L, E> getEventLog(Class<?> type, String name) {
    return eventRegistries.computeIfAbsent(type, t -> new ConcurrentHashMap<>()).get(name);
  }

  /**
   * Returns an event log, creating a new log if none exists.
   *
   * @param type the log type
   * @param name the log name
   * @param <L> the listener type
   * @param <E> the event type
   * @return the event log
   */
  @SuppressWarnings("unchecked")
  public <L, E> EventLog<L, E> getOrCreateEventLog(Class<?> type, String name, Function<EventLog<L, E>, L> listenerFactory) {
    return eventRegistries.computeIfAbsent(type, t -> new ConcurrentHashMap<>())
        .computeIfAbsent(name, n -> new EventLog<>(listenerFactory));
  }

  /**
   * Removes and returns the given event log.
   *
   * @param type the log type
   * @param name the log name
   * @param <L> the listener type
   * @param <E> the event type
   * @return the removed event log or {@code null} if non exits
   */
  @SuppressWarnings("unchecked")
  public <L, E> EventLog<L, E> removeEventLog(Class<?> type, String name) {
    return eventRegistries.computeIfAbsent(type, t -> new ConcurrentHashMap<>()).remove(name);
  }

  /**
   * Returns event log names for the given type.
   *
   * @param type the type for which to return event log names
   * @return event log names for the given type
   */
  public Set<String> getEventLogNames(Class<?> type) {
    return eventRegistries.computeIfAbsent(type, t -> new ConcurrentHashMap<>()).keySet();
  }

}
