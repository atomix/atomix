/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.EventListener;

import java.util.concurrent.CompletableFuture;

/**
 * Protocol connection.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ProtocolConnection extends ProtocolReader, ProtocolWriter {

  /**
   * Registers a connection close listener.
   *
   * @param listener A connection close listener.
   * @return The protocol connection.
   */
  ProtocolConnection closeListener(EventListener<Void> listener);

  /**
   * Registers a connection exception listener.
   *
   * @param listener A connection exception listener.
   * @return The protocol connection.
   */
  ProtocolConnection exceptionListener(EventListener<Throwable> listener);

  /**
   * Closes the connection.
   *
   * @return A completable future to be completed once the connection is closed.
   */
  CompletableFuture<Void> close();

}
