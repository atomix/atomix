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
package net.kuujo.copycat.cluster;

/**
 * Local cluster member.<p>
 *
 * The local member provides an interface for receiving messages from other members of the cluster. Messages can
 * be sent across the cluster via a topic-based system.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LocalMember extends Member {

  /**
   * Registers a message type registerHandler on the local member.
   *
   * @param topic The topic to handle.
   * @param handler The message registerHandler.
   * @param <T> The request message type.
   * @param <U> The response message type.
   * @return The local member.
   */
  <T, U> LocalMember registerHandler(String topic, MessageHandler<T, U> handler);

  /**
   * Unregisters a message type registerHandler on the local member.
   *
   * @param topic The topic to handle.
   * @return The local member.
   */
  LocalMember unregisterHandler(String topic);

}
