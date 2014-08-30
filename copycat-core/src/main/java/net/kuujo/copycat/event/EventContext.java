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
package net.kuujo.copycat.event;

/**
 * Event context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <E> The event type.
 */
public interface EventContext<E extends Event> {

  /**
   * Runs an event handler when the event occurs.
   *
   * @param handler The event handler to run.
   * @return The event context.
   */
  EventContext<E> run(EventHandler<E> handler);

  /**
   * Asynchronously runs an event handler when the event occurs.
   *
   * @param handler The event handler to run.
   * @return The event context.
   */
  EventContext<E> runAsync(EventHandler<E> handler);

  /**
   * Runs an event handler once when the event occurs.
   *
   * @param handler The event handler to run.
   * @return The event context.
   */
  EventContext<E> runOnce(EventHandler<E> handler);

  /**
   * Asynchronously runs an event handler once when the event occurs.
   *
   * @param handler The event handler to run.
   * @return The event context.
   */
  EventContext<E> runOnceAsync(EventHandler<E> handler);

}
