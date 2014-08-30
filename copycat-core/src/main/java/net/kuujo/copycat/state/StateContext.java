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
package net.kuujo.copycat.state;

import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.CopyCatConfig;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.event.Events;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.registry.Registry;

/**
 * Container for replica state information.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateContext {

  /**
   * Returns the internal state cluster.
   *
   * @return The internal state cluster.
   */
  Cluster cluster();

  /**
   * Returns the copycat configuration.
   *
   * @return The copycat configuration.
   */
  CopyCatConfig config();

  /**
   * Returns the state log.
   *
   * @return The state log.
   */
  Log log();

  /**
   * Returns the state machine.
   *
   * @return The state machine.
   */
  StateMachine stateMachine();

  /**
   * Returns the registry.
   *
   * @return The registry.
   */
  Registry registry();

  /**
   * Returns the state context events.
   *
   * @return State context events.
   */
  Events events();

  /**
   * Returns the current state.
   *
   * @return The current state.
   */
  State.Type state();

  /**
   * Returns the current leader.
   *
   * @return The current leader.
   */
  String leader();

  /**
   * Returns a boolean indicating whether the state is leader.
   *
   * @return Indicates whether the current state is leader.
   */
  boolean isLeader();

  /**
   * Starts the context.
   *
   * @return A completable future to be completed once started.
   */
  CompletableFuture<Void> start();

  /**
   * Stops the context.
   *
   * @return A completable future to be completed once stopped.
   */
  CompletableFuture<Void> stop();

  /**
   * Submits a command to the context.
   *
   * @param command The command to submit.
   * @param args The command arguments.
   * @return A completable future to be completed with the command result.
   */
  <T> CompletableFuture<T> submitCommand(String command, Object... args);

}
