// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.event;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Event utilities.
 */
public final class Events {

  /**
   * Returns the collection of events provided by the given service interface.
   *
   * @param serviceInterface the client service interface
   * @return the events provided by the given service interface
   */
  public static Map<Method, EventType> getMethodMap(Class<?> serviceInterface) {
    if (!serviceInterface.isInterface()) {
      Map<Method, EventType> events = new HashMap<>();
      for (Class<?> iface : serviceInterface.getInterfaces()) {
        events.putAll(findMethods(iface));
      }
      return events;
    }
    return findMethods(serviceInterface);
  }

  /**
   * Recursively finds events defined by the given type and its implemented interfaces.
   *
   * @param type the type for which to find events
   * @return the events defined by the given type and its parent interfaces
   */
  private static Map<Method, EventType> findMethods(Class<?> type) {
    Map<Method, EventType> events = new HashMap<>();
    for (Method method : type.getDeclaredMethods()) {
      Event event = method.getAnnotation(Event.class);
      if (event != null) {
        String name = "".equals(event.value()) ? method.getName() : event.value();
        events.put(method, EventType.from(name));
      }
    }
    for (Class<?> iface : type.getInterfaces()) {
      events.putAll(findMethods(iface));
    }
    return events;
  }

  /**
   * Returns the collection of events provided by the given service interface.
   *
   * @param serviceInterface the service interface
   * @return the events provided by the given service interface
   */
  public static Map<EventType, Method> getEventMap(Class<?> serviceInterface) {
    if (!serviceInterface.isInterface()) {
      Class type = serviceInterface;
      Map<EventType, Method> events = new HashMap<>();
      while (type != Object.class) {
        for (Class<?> iface : type.getInterfaces()) {
          events.putAll(findEvents(iface));
        }
        type = type.getSuperclass();
      }
      return events;
    }
    return findEvents(serviceInterface);
  }

  /**
   * Recursively finds events defined by the given type and its implemented interfaces.
   *
   * @param type the type for which to find events
   * @return the events defined by the given type and its parent interfaces
   */
  private static Map<EventType, Method> findEvents(Class<?> type) {
    Map<EventType, Method> events = new HashMap<>();
    for (Method method : type.getDeclaredMethods()) {
      Event event = method.getAnnotation(Event.class);
      if (event != null) {
        String name = "".equals(event.value()) ? method.getName() : event.value();
        events.put(EventType.from(name), method);
      }
    }
    for (Class<?> iface : type.getInterfaces()) {
      events.putAll(findEvents(iface));
    }
    return events;
  }

  private Events() {
  }
}
