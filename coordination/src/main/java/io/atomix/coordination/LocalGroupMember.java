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
 * limitations under the License
 */
package io.atomix.coordination;

import io.atomix.catalyst.util.Listener;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Local group member.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface LocalGroupMember extends GroupMember {

  /**
   * Sets the value of a property of the member.
   *
   * @param property The property to set.
   * @param value The value of the property to set.
   * @return A completable future to be completed once the value has been set.
   */
  CompletableFuture<Void> set(String property, Object value);

  /**
   * Removes a property of the member.
   *
   * @param property The property to remove.
   * @return A completable future to be completed once the property has been removed.
   */
  CompletableFuture<Void> remove(String property);

  /**
   * Handles a message to the member.
   *
   * @param topic The message topic.
   * @param consumer The message consumer.
   * @param <T> The message type.
   * @return The local group member.
   */
  <T> Listener<T> onMessage(String topic, Consumer<T> consumer);

}
