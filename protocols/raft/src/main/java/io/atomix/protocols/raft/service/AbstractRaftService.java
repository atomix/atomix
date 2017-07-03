/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.service;

import io.atomix.protocols.raft.operation.RaftOperationExecutor;
import io.atomix.protocols.raft.operation.impl.DefaultRaftOperationExecutor;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSessions;
import io.atomix.time.LogicalClock;
import io.atomix.time.WallClock;
import io.atomix.utils.concurrent.Scheduler;

/**
 * Raft service.
 */
public abstract class AbstractRaftService implements RaftService {
  private ServiceContext context;
  private RaftOperationExecutor executor;

  @Override
  public void init(ServiceContext context) {
    this.context = context;
    this.executor = new DefaultRaftOperationExecutor(context);
    configure(executor);
  }

  @Override
  public byte[] apply(RaftCommit<byte[]> commit) {
    return executor.apply(commit);
  }

  /**
   * Configures the state machine.
   * <p>
   * By default, this method will configure state machine operations by extracting public methods with
   * a single {@link RaftCommit} parameter via reflection. Override this method to explicitly register
   * state machine operations via the provided {@link RaftOperationExecutor}.
   *
   * @param executor The state machine executor.
   */
  protected abstract void configure(RaftOperationExecutor executor);

  /**
   * Returns the service context.
   *
   * @return the service context
   */
  protected ServiceContext getContext() {
    return context;
  }

  /**
   * Returns the state machine scheduler.
   *
   * @return The state machine scheduler.
   */
  protected Scheduler getScheduler() {
    return executor;
  }

  /**
   * Returns the unique state machine identifier.
   *
   * @return The unique state machine identifier.
   */
  protected ServiceId getStateMachineId() {
    return context.serviceId();
  }

  /**
   * Returns the unique state machine name.
   *
   * @return The unique state machine name.
   */
  protected String getName() {
    return context.serviceName();
  }

  /**
   * Returns the state machine's current index.
   *
   * @return The state machine's current index.
   */
  protected long getCurrentIndex() {
    return context.currentIndex();
  }

  /**
   * Returns the state machine's wall clock.
   *
   * @return The state machine's wall clock.
   */
  protected WallClock getWallClock() {
    return context.wallClock();
  }

  /**
   * Returns the state machine's logical clock.
   *
   * @return The state machine's logical clock.
   */
  protected LogicalClock getLogicalClock() {
    return context.logicalClock();
  }

  /**
   * Returns the sessions registered with the state machines.
   *
   * @return The state machine's sessions.
   */
  protected RaftSessions getSessions() {
    return context.sessions();
  }

  @Override
  public void onOpen(RaftSession session) {

  }

  @Override
  public void onExpire(RaftSession session) {

  }

  @Override
  public void onClose(RaftSession session) {

  }
}
