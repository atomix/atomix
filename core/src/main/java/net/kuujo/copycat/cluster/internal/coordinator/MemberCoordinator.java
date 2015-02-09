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
package net.kuujo.copycat.cluster.internal.coordinator;

import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.cluster.Member;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Cluster member coordinator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface MemberCoordinator extends Managed<MemberCoordinator> {

  /**
   * Returns the member URI.
   *
   * @return The member URI.
   */
  String uri();

  /**
   * Returns the member type.
   *
   * @return The member type.
   */
  Member.Type type();

  /**
   * Returns the member state.
   *
   * @return The member state.
   */
  Member.Status state();

  /**
   * Sends an internal message.
   *
   * @param topic The topic to which to send the message.
   * @param address The internal address to which to send the message.
   * @param id The internal handler to which to send the message.
   * @param message The message to send.
   * @return A completable future to be completed with the message result.
   */
  CompletableFuture<ByteBuffer> send(String topic, int address, int id, ByteBuffer message);

}
