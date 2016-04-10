/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.messaging;

import io.atomix.catalyst.util.Listener;

import java.util.function.Consumer;

/**
 * Message consumer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface MessageConsumer<T> extends AutoCloseable {

  /**
   * Message consumer options.
   */
  class Options {
  }

  /**
   * Registers a callback for handling message messages.
   *
   * @param callback The message listener callback.
   * @return The message listener.
   */
  Listener<Message<T>> onMessage(Consumer<Message<T>> callback);

  @Override
  default void close() {
  }

}
