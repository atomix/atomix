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
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.cluster.LocalMember;
import net.kuujo.copycat.cluster.MessageHandler;

/**
 * Internal local cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface InternalLocalMember extends InternalMember, LocalMember {

  /**
   * Registers a message handler.
   *
   * @param topic The topic for which to register the handler.
   * @param address The internal address at which to register the handler.
   * @param handler The handler to register.
   * @return The internal local member.
   */
  <T, U> InternalLocalMember register(String topic, int address, MessageHandler<T, U> handler);

  /**
   * Unregisters a message handler.
   *
   * @param topic The topic for which to unregister the handler.
   * @param address The internal address at which to unregister the handler.
   * @return The internal local member.
   */
  InternalLocalMember unregister(String topic, int address);

}
