/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat;

import net.kuujo.copycat.event.*;

/**
 * Base copycat interface.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface BaseCopycat<T extends BaseCopycatContext> {

  /**
   * Returns the copycat context.
   *
   * @return The underlying copycat context.
   */
  T context();

  /**
   * Returns the context events.
   *
   * @return Context events.
   */
  Events on();

  /**
   * Returns the context for a specific event.
   *
   * @param event The event for which to return the context.
   * @return The event context.
   * @throws NullPointerException if {@code event} is null
   */
  <T extends Event> EventContext<T> on(Class<T> event);

  /**
   * Returns the event handlers registry.
   *
   * @return The event handlers registry.
   */
  EventHandlers events();

  /**
   * Returns an event handler registry for a specific event.
   *
   * @param event The event for which to return the registry.
   * @return An event handler registry.
   * @throws NullPointerException if {@code event} is null
   */
  <T extends Event> EventHandlerRegistry<T> event(Class<T> event);

}
