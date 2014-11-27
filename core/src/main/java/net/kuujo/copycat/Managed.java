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

import java.util.concurrent.CompletableFuture;

/**
 * Interface for types that can be asynchronously opened and closed.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Managed {

  /**
   * Opens the managed object.
   *
   * @return A completable future to be completed once the object has been opened.
   */
  CompletableFuture<Void> open();

  /**
   * Closes the managed object.
   *
   * @return A completable future to be completed once the object has been closed.
   */
  CompletableFuture<Void> close();

}
