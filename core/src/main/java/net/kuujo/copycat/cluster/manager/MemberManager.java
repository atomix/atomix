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
package net.kuujo.copycat.cluster.manager;

import net.kuujo.copycat.cluster.Member;

import java.util.concurrent.CompletableFuture;

/**
 * Cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface MemberManager extends Member {

  /**
   * Sends a message to the member.
   *
   * @param topic The message topic.
   * @param id The handler to which to send the message.
   * @param message The message to send.
   * @param <T> The message type.
   * @param <U> The response type.
   * @return A completable future to be completed with the message response.
   */
  <T, U> CompletableFuture<U> send(String topic, int id, T message);

}
