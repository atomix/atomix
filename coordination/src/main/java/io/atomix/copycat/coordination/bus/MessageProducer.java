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
package io.atomix.copycat.coordination.bus;

import java.util.concurrent.CompletableFuture;

/**
 * Message producer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface MessageProducer<T> {

  /**
   * Sends a message.
   *
   * @param message The message to send.
   * @param <U> The message response type.
   * @return A completable future to be completed once the message has been received.
   */
  <U> CompletableFuture<U> send(T message);

  /**
   * Closes the producer.
   *
   * @return A completable future to be completed once the producer is closed.
   */
  CompletableFuture<Void> close();

}
