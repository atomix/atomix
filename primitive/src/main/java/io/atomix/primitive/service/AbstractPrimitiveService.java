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

import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.Operations;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.primitive.session.PrimitiveSession;
import io.atomix.primitive.session.PrimitiveSessions;
import io.atomix.utils.concurrent.Scheduler;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.time.Clock;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft service.
 */
public abstract class AbstractPrimitiveService<S, C extends ServiceConfig> implements PrimitiveService {
  private final Class<S> serviceInterface;
  private final C config;
  private Logger log;
  private ServiceContext context;
  private ServiceExecutor executor;

  protected AbstractPrimitiveService(C config) {
    this(null, config);
  }

  protected AbstractPrimitiveService(Class<S> serviceInterface, C config) {
    this.serviceInterface = serviceInterface;
    this.config = config;
  }

  /**
   * Encodes the given object using the configured {@link #serializer()}.
   *
   * @param object the object to encode
   * @param <T>    the object type
   * @return the encoded bytes
   */
  protected <T> byte[] encode(T object) {
    return object != null ? serializer().encode(object) : null;
  }

  /**
   * Decodes the given object using the configured {@link #serializer()}.
   *
   * @param bytes the bytes to decode
   * @param <T>   the object type
   * @return the decoded object
   */
  protected <T> T decode(byte[] bytes) {
    return bytes != null ? serializer().decode(bytes) : null;
  }

  @Override
  public void init(ServiceContext context) {
    this.context = context;
    this.executor = new DefaultServiceExecutor(context, serializer());
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
  protected void configure(ServiceExecutor executor) {
    checkNotNull(serviceInterface);
    for (Map.Entry<OperationId, Method> entry : Operations.getOperationMap(serviceInterface).entrySet()) {
      configure(entry.getKey(), entry.getValue(), executor);
    }
  }

  /**
   * Configures the given operation on the given executor.
   *
   * @param operationId the operation identifier
   * @param method      the operation method
   * @param executor    the service executor
   */
  private void configure(OperationId operationId, Method method, ServiceExecutor executor) {
    if (method.getReturnType() == Void.TYPE) {
      if (method.getParameterTypes().length == 0) {
        executor.register(operationId, () -> {
          try {
            method.invoke(this);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new PrimitiveException.ServiceException(e.getMessage());
          }
        });
      } else {
        executor.register(operationId, args -> {
          try {
            method.invoke(this, (Object[]) args.value());
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new PrimitiveException.ServiceException(e.getMessage());
          }
        });
      }
    } else {
      if (method.getParameterTypes().length == 0) {
        executor.register(operationId, () -> {
          try {
            return method.invoke(this);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new PrimitiveException.ServiceException(e.getMessage());
          }
        });
      } else {
        executor.register(operationId, args -> {
          try {
            return method.invoke(this, (Object[]) args.value());
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new PrimitiveException.ServiceException(e.getMessage());
          }
        });
      }
    }
  }

  /**
   * Returns the primitive type.
   *
   * @return the primitive type
   */
  protected PrimitiveType getPrimitiveType() {
    return context.serviceType();
  }

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
   * Returns the service configuration.
   *
   * @return the service configuration
   */
  protected C getServiceConfig() {
    return config;
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
  protected PrimitiveSession getCurrentSession() {
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
  protected PrimitiveSessions getSessions() {
    return context.sessions();
  }

  @Override
  public void onOpen(PrimitiveSession session) {

  }

  @Override
  public void onExpire(PrimitiveSession session) {

  }

  @Override
  public void onClose(PrimitiveSession session) {

  }
}
