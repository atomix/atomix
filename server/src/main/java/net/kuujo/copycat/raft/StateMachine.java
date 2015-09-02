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
package net.kuujo.copycat.raft;

import net.kuujo.copycat.raft.protocol.Command;
import net.kuujo.copycat.raft.protocol.Operation;
import net.kuujo.copycat.raft.protocol.Query;
import net.kuujo.copycat.raft.session.Session;
import net.kuujo.copycat.raft.session.Sessions;
import net.kuujo.copycat.util.Assert;

import java.time.Clock;
import java.time.Instant;

/**
 * Base class for user-provided Raft state machines.
 * <p>
 * Users should extend this class to create a state machine for use within a {@link RaftServer}.
 * <p>
 * State machines are responsible for handling {@link Operation operations} submitted to the Raft
 * cluster and filtering {@link Commit committed} operations out of the Raft log. The most
 * important rule of state machines is that <em>state machines must be deterministic</em> in order to maintain Copycat's
 * consistency guarantees. That is, state machines must not change their behavior based on external influences and have
 * no side effects. Users should <em>never</em> use {@code System} time to control behavior within a state machine.
 * <p>
 * When {@link Command commands} and {@link Query queries} are submitted
 * to the Raft cluster, the {@link RaftServer} will log and replicate them as necessary
 * and, once complete, apply them to the configured state machine.
 *
 * @see Commit
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class StateMachine implements AutoCloseable {
  private StateMachineContext context;

  protected StateMachine() {
  }

  /**
   * Initializes the state machine.
   *
   * @param context The state machine context.
   * @throws NullPointerException if {@code context} is null
   */
  public void init(StateMachineContext context) {
    this.context = Assert.notNull(context, "context");
  }

  /**
   * Configures the state machine.
   *
   * @param executor The state machine executor.
   */
  public abstract void configure(StateMachineExecutor executor);

  /**
   * Returns the state machine sessions.
   *
   * @return The state machine sessions.
   */
  protected Sessions sessions() {
    return context.sessions();
  }

  /**
   * Returns the state machine's deterministic clock.
   *
   * @return The state machine's deterministic clock.
   */
  protected Clock clock() {
    return context.clock();
  }

  /**
   * Returns the current state machine time.
   *
   * @return The current state machine time.
   */
  protected Instant now() {
    return context.now();
  }

  /**
   * Called when a new session is registered.
   *
   * @param session The session that was registered.
   */
  public void register(Session session) {

  }

  /**
   * Called when a session is expired.
   *
   * @param session The expired session.
   */
  public void expire(Session session) {

  }

  /**
   * Called when a session is closed.
   *
   * @param session The session that was closed.
   */
  public void close(Session session) {

  }

  /**
   * Closes the state machine.
   */
  @Override
  public void close() {

  }

}
