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
package net.kuujo.copycat;

/**
 * Copycat state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateContext<T extends State> {

  /**
   * Returns the current state.
   *
   * @return The current state.
   */
  T state();

  /**
   * Puts a value in the state context.
   *
   * @param key The state key.
   * @param value The state value.
   * @return The state context.
   */
  StateContext<T> put(String key, Object value);

  /**
   * Gets a value from the state context.
   *
   * @param key The state key.
   * @param <U> The state value type.
   * @return The state value.
   */
  <U> U get(String key);

  /**
   * Removes a value from the state context.
   *
   * @param key The state key.
   * @param <U> The state value type.
   * @return The removed state value.
   */
  <U> U remove(String key);

  /**
   * Clears state from the state context.
   *
   * @return The state context.
   */
  StateContext<T> clear();

  /**
   * Transitions the context to a new state.
   *
   * @param state The state to which to transition.
   * @return The state context.
   */
  StateContext<T> transition(T state);

}
