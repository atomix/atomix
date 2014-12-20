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
package net.kuujo.copycat.cluster.coordinator;

import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.spi.ExecutionContext;

/**
 * Local member coordinator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LocalMemberCoordinator extends MemberCoordinator {

  /**
   * Registers a message registerHandler.
   *
   * @param topic The topic for which to register the registerHandler.
   * @param address The internal address at which to register the registerHandler.
   * @param handler The registerHandler to register.
   * @return The local member coordinator.
   */
  <T, U> LocalMemberCoordinator register(String topic, int address, MessageHandler<T, U> handler);

  /**
   * Unregisters a message registerHandler.
   *
   * @param topic The topic for which to unregister the registerHandler.
   * @param address The internal address at which to unregister the registerHandler.
   * @return The local member coordinator.
   */
  LocalMemberCoordinator unregister(String topic, int address);

  /**
   * Registers an execution context for the given address.
   *
   * @param address The address for which to register the context.
   * @param context The execution context.
   * @return The local member coordinator.
   */
  LocalMemberCoordinator registerExecutor(int address, ExecutionContext context);

  /**
   * Unregisters an execution context for the given address.
   *
   * @param address The address for which to unregister the context.
   * @return The local member coordinator.
   */
  LocalMemberCoordinator unregisterExecutor(int address);
}
