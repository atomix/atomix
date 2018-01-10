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
package io.atomix.primitive.service;

import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.Sessions;
import io.atomix.utils.concurrent.Scheduler;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.time.Clock;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;

/**
 * Raft service.
 */
public abstract class AbstractPrimitiveService implements PrimitiveService {
  private Logger log;
  private ServiceContext context;
  private ServiceExecutor executor;

  @Override
  public void init(ServiceContext context) {
    this.context = context;
    this.executor = new DefaultServiceExecutor(context);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveService.class)
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
   * state machine operations via the provided {@link ServiceExecutor}.
   *
   * @param executor The state machine executor.
   */
  protected abstract void configure(ServiceExecutor executor);

  /**
   * Returns the service logger.
   *
   * @return the service logger
   */
  protected Logger getLogger() {
    return log;
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
  protected PrimitiveId getServiceId() {
    return context.serviceId();
  }

  /**
   * Returns the unique state machine name.
   *
   * @return The unique state machine name.
   */
  protected String getServiceName() {
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
   * Returns the current session.
   *
   * @return the current session
   */
  protected Session getCurrentSession() {
    return context.currentSession();
  }

  /**
   * Returns the state machine's clock.
   *
   * @return The state machine's clock.
   */
  protected Clock getClock() {
    return getWallClock();
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
  protected Sessions getSessions() {
    return context.sessions();
  }

  @Override
  public void onOpen(Session session) {

  }

  @Override
  public void onExpire(Session session) {

  }

  @Override
  public void onClose(Session session) {

  }
}
