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
package net.kuujo.copycat.collections.internal.collection;

import net.kuujo.copycat.State;
import net.kuujo.copycat.StateContext;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous collection state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncCollectionState<T extends AsyncCollectionState<T, U>, U> extends State {

  /**
   * Adds a entry to the collection.
   *
   * @param value The entry to add.
   * @param context The state context.
   */
  boolean add(U value, StateContext<T> context);

  /**
   * Removes a entry from the collection.
   *
   * @param value The entry to remove.
   * @param context The state context.
   */
  boolean remove(U value, StateContext<T> context);

  /**
   * Checks whether the collection contains a entry.
   *
   * @param value The entry to check.
   * @param context The state context.
   */
  boolean contains(Object value, StateContext<T> context);

  /**
   * Gets the current collection size.
   *
   * @param context The state context.
   */
  int size(StateContext<T> context);

  /**
   * Checks whether the collection is empty.
   *
   * @param context The state context.
   */
  boolean isEmpty(StateContext<T> context);

  /**
   * Clears all values from the collection.
   *
   * @param context The state context.
   */
  void clear(StateContext<T> context);

}
