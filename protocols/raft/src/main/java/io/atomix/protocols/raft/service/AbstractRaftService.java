/*
 * Copyright 2017-present Open Networking Foundation
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

import io.atomix.protocols.raft.service.impl.DefaultRaftServiceExecutor;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSessions;
import io.atomix.time.Clock;
import io.atomix.time.LogicalClock;
import io.atomix.time.WallClock;
import io.atomix.time.WallClockTimestamp;
import io.atomix.utils.concurrent.Scheduler;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

/**
 * Raft service.
 */
public abstract class AbstractRaftService implements RaftService {
  private Logger log;
  private ServiceContext context;
  private RaftServiceExecutor executor;

  @Override
  public void init(ServiceContext context) {
    this.context = context;
    this.executor = new DefaultRaftServiceExecutor(context);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftService.class)
        .addValue(context.serviceId())
        .add("type", context.serviceType())
        .add("name", context.serviceName())
        .build());
    configure(executor);
  }

  @Override
  public void tick(WallClockTimestamp timestamp) {
    executor.tick(timestamp);
  }

  @Override
  public byte[] apply(Commit<byte[]> commit) {
    return executor.apply(commit);
  }

  /**
   * Configures the state machine.
   * <p>
   * By default, this method will configure state machine operations by extracting public methods with
   * a single {@link Commit} parameter via reflection. Override this method to explicitly register
   * state machine operations via the provided {@link RaftServiceExecutor}.
   *
   * @param executor The state machine executor.
   */
  protected abstract void configure(RaftServiceExecutor executor);

  /**
   * Returns the service context.
   *
   * @return the service context
   */
  protected ServiceContext context() {
    return context;
  }

  /**
   * Returns the service logger.
   *
   * @return the service logger
   */
  protected Logger logger() {
    return log;
  }

  /**
   * Returns the state machine scheduler.
   *
   * @return The state machine scheduler.
   */
  protected Scheduler scheduler() {
    return executor;
  }

  /**
   * Returns the unique state machine identifier.
   *
   * @return The unique state machine identifier.
   */
  protected ServiceId serviceId() {
    return context.serviceId();
  }

  /**
   * Returns the unique state machine name.
   *
   * @return The unique state machine name.
   */
  protected String serviceName() {
    return context.serviceName();
  }

  /**
   * Returns the state machine's current index.
   *
   * @return The state machine's current index.
   */
  protected long currentIndex() {
    return context.currentIndex();
  }

  /**
   * Returns the state machine's clock.
   *
   * @return The state machine's clock.
   */
  protected Clock clock() {
    return wallClock();
  }

  /**
   * Returns the state machine's wall clock.
   *
   * @return The state machine's wall clock.
   */
  protected WallClock wallClock() {
    return context.wallClock();
  }

  /**
   * Returns the state machine's logical clock.
   *
   * @return The state machine's logical clock.
   */
  protected LogicalClock logicalClock() {
    return context.logicalClock();
  }

  /**
   * Returns the sessions registered with the state machines.
   *
   * @return The state machine's sessions.
   */
  protected RaftSessions sessions() {
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
