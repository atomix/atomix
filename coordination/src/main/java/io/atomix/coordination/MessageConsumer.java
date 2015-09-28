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
package io.atomix.coordination;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Message consumer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface MessageConsumer<T> {

  /**
   * Sets a message consumer callback.
   *
   * @param consumer The consumer callback.
   * @return The message consumer.
   */
  MessageConsumer<T> onMessage(Function<T, ?> consumer);

  /**
   * Closes the consumer.
   *
   * @return A completable future to be completed once the consumer has been closed.
   */
  CompletableFuture<Void> close();

}
