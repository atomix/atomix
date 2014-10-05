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
package net.kuujo.copycat.event.impl;

import java.util.HashSet;
import java.util.Set;

import net.kuujo.copycat.event.Event;
import net.kuujo.copycat.event.EventHandler;
import net.kuujo.copycat.event.EventHandlerRegistry;

/**
 * Default event handler registry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <E> The event type.
 */
public class DefaultEventHandlerRegistry<E extends Event> implements EventHandlerRegistry<E>, EventHandler<E> {
  private final Set<EventHandler<E>> handlers = new HashSet<>(10);

  @Override
  public EventHandlerRegistry<E> registerHandler(EventHandler<E> handler) {
    handlers.add(handler);
    return this;
  }

  @Override
  public EventHandlerRegistry<E> unregisterHandler(EventHandler<E> handler) {
    handlers.remove(handler);
    return this;
  }

  @Override
  public void handle(E event) {
    for (EventHandler<E> handler : handlers) {
      handler.handle(event);
    }
  }

  @Override
  public String toString() {
    return String.format("%s[handlers=%s]", getClass().getSimpleName(), handlers);
  }

}
