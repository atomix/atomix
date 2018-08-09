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
