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
package net.kuujo.copycat.cluster;

/**
 * Default local member implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LocalMember extends Member {

  /**
   * Registers a message handler on the local member.<p>
   *
   * The message handler can be used to receive direct messages from other members of the resource cluster. Messages
   * are sent between members of the cluster using a topic based messaging system. Handlers registered on this local
   * member instance apply only to messaging within the resource to which the cluster belongs. Message handlers
   * registered on other resource clusters cannot receive messages from members in this cluster and vice versa. Only
   * one handler for any given topic for a cluster can be registered at any given time.
   *
   * @param type The message type for which to register the handler.
   * @param handler The message handler to register. This handler will be invoked whenever a message is received for
   *                the given type. The message handler should return a {@link java.util.concurrent.CompletableFuture}
   *                that will be completed with the message response.
   * @param <T> The request message type.
   * @param <U> The response message type.
   * @return The local member.
   */
  <T, U> LocalMember registerHandler(Class<? super T> type, MessageHandler<T, U> handler);

  /**
   * Registers a message handler on the local member.<p>
   *
   * The message handler can be used to receive direct messages from other members of the resource cluster. Messages
   * are sent between members of the cluster using a topic based messaging system. Handlers registered on this local
   * member instance apply only to messaging within the resource to which the cluster belongs. Message handlers
   * registered on other resource clusters cannot receive messages from members in this cluster and vice versa. Only
   * one handler for any given topic for a cluster can be registered at any given time.
   *
   * @param topic The topic for which to register the handler. Messages sent to this member via the given topic will
   *              be handled by the given message handler.
   * @param handler The message handler to register. This handler will be invoked whenever a message is received for
   *                the given topic. The message handler should return a {@link java.util.concurrent.CompletableFuture}
   *                that will be completed with the message response.
   * @param <T> The request message type.
   * @param <U> The response message type.
   * @return The local member.
   */
  <T, U> LocalMember registerHandler(String topic, MessageHandler<T, U> handler);

  /**
   * Unregisters a message handler on the local member.
   *
   * @param type The type for which to unregister the handler.
   * @return The local member.
   */
  LocalMember unregisterHandler(Class<?> type);

  /**
   * Unregisters a message handler on the local member.
   *
   * @param topic The topic for which to unregister the handler.
   * @return The local member.
   */
  LocalMember unregisterHandler(String topic);

}
