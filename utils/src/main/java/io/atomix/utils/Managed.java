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
package io.atomix.utils;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for types that can be asynchronously opened and closed.
 *
 * @param <T> managed type
 */
public interface Managed<T> {

  /**
   * Opens the managed object.
   *
   * @return A completable future to be completed once the object has been opened.
   */
  CompletableFuture<T> open();

  /**
   * Returns a boolean value indicating whether the managed object is open.
   *
   * @return Indicates whether the managed object is open.
   */
  boolean isOpen();

  /**
   * Closes the managed object.
   *
   * @return A completable future to be completed once the object has been closed.
   */
  CompletableFuture<Void> close();

  /**
   * Returns a boolean value indicating whether the managed object is closed.
   *
   * @return Indicates whether the managed object is closed.
   */
  boolean isClosed();

}
