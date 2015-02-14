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
package net.kuujo.copycat.state;

import net.kuujo.copycat.cluster.Cluster;

/**
 * Copycat status context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateContext<T> {

  /**
   * Returns the current status cluster.
   *
   * @return The current status cluster.
   */
  Cluster cluster();

  /**
   * Returns the current status.
   *
   * @return The current status.
   */
  T state();

  /**
   * Puts a value in the status context.
   *
   * @param key The status key.
   * @param value The status value.
   * @return The status context.
   */
  StateContext<T> put(String key, Object value);

  /**
   * Gets a value from the status context.
   *
   * @param key The status key.
   * @param <U> The status value type.
   * @return The status value.
   */
  <U> U get(String key);

  /**
   * Removes a value from the status context.
   *
   * @param key The status key.
   * @param <U> The status value type.
   * @return The removed status value.
   */
  <U> U remove(String key);

  /**
   * Clears status from the status context.
   *
   * @return The status context.
   */
  StateContext<T> clear();

  /**
   * Transitions the context to a new status.
   *
   * @param state The status to which to transition.
   * @return The status context.
   */
  StateContext<T> transition(T state);

}
