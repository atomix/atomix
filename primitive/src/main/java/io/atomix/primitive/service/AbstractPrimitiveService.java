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

import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.Events;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.Operations;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.primitive.session.PrimitiveSession;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Scheduler;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.time.Clock;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft service.
 */
public abstract class AbstractPrimitiveService<S, C, F extends ServiceConfig> implements PrimitiveService {
  private final Class<S> serviceInterface;
  private final Class<C> clientInterface;
  private final F config;
  private Logger log;
  private ServiceContext context;
  private ServiceExecutor executor;
  private final Map<SessionId, SessionProxy> sessions = Maps.newHashMap();

  protected AbstractPrimitiveService(F config) {
    this(null, null, config);
  }

  protected AbstractPrimitiveService(Class<S> serviceInterface, Class<C> clientInterface, F config) {
    this.serviceInterface = serviceInterface;
    this.clientInterface = clientInterface;
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
  public final void init(ServiceContext context) {
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
  public final void tick(WallClockTimestamp timestamp) {
    executor.tick(timestamp);
  }

  @Override
  public final byte[] apply(Commit<byte[]> commit) {
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
    Operations.getOperationMap(serviceInterface).forEach(((operationId, method) -> configure(operationId, method, executor)));
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
  protected F getServiceConfig() {
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
   * Returns the session with the given identifier.
   *
   * @param sessionId the session identifier
   * @return the primitive session
   */
  protected PrimitiveSession getSession(long sessionId) {
    return getSession(SessionId.from(sessionId));
  }

  /**
   * Returns the session with the given identifier.
   *
   * @param sessionId the session identifier
   * @return the primitive session
   */
  protected PrimitiveSession getSession(SessionId sessionId) {
    SessionProxy sessionProxy = sessions.get(sessionId);
    return sessionProxy != null ? sessionProxy.session : null;
  }

  /**
   * Returns the collection of open sessions.
   *
   * @return the collection of open sessions
   */
  protected Collection<PrimitiveSession> getSessions() {
    return sessions.values().stream().map(sessionProxy -> sessionProxy.session).collect(Collectors.toList());
  }

  /**
   * Publishes an event to the given session.
   *
   * @param sessionId the session to which to publish the event
   * @param event     the event to publish
   */
  protected void acceptOn(SessionId sessionId, Consumer<C> event) {
    SessionProxy sessionProxy = sessions.get(sessionId);
    if (sessionProxy != null) {
      sessionProxy.accept(event);
    }
  }

  /**
   * Publishes an event to all sessions.
   *
   * @param event the event to publish
   */
  protected void acceptAll(Consumer<C> event) {
    for (SessionProxy sessionProxy : sessions.values()) {
      sessionProxy.accept(event);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public final void register(PrimitiveSession session) {
    SessionProxyHandler sessionProxyHandler = new SessionProxyHandler(session);
    if (clientInterface != null) {
      C sessionProxy = (C) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{clientInterface}, sessionProxyHandler);
      sessions.put(session.sessionId(), new SessionProxy(session, sessionProxy));
    } else {
      sessions.put(session.sessionId(), new SessionProxy(session, null));
    }
    onOpen(session);
  }

  @Override
  public final void expire(SessionId sessionId) {
    SessionProxy session = sessions.remove(sessionId);
    if (session != null) {
      onExpire(session.session);
    }
  }

  @Override
  public final void close(SessionId sessionId) {
    SessionProxy session = sessions.remove(sessionId);
    if (session != null) {
      onClose(session.session);
    }
  }

  /**
   * Called when a new session is registered.
   * <p>
   * A session is registered when a new client connects to the cluster or an existing client recovers its
   * session after being partitioned from the cluster. It's important to note that when this method is called,
   * the {@link PrimitiveSession} is <em>not yet open</em> and so events cannot be {@link PrimitiveSession#publish(PrimitiveEvent) published}
   * to the registered session. This is because clients cannot reliably track messages pushed from server state machines
   * to the client until the session has been fully registered. Session event messages may still be published to
   * other already-registered sessions in reaction to a session being registered.
   * <p>
   * To push session event messages to a client through its session upon registration, state machines can
   * use an asynchronous callback or schedule a callback to send a message.
   * <pre>
   *   {@code
   *   public void onOpen(RaftSession session) {
   *     executor.execute(() -> session.publish("foo", "Hello world!"));
   *   }
   *   }
   * </pre>
   * Sending a session event message in an asynchronous callback allows the server time to register the session
   * and notify the client before the event message is sent. Published event messages sent via this method will
   * be sent the next time an operation is applied to the state machine.
   *
   * @param session The session that was registered. State machines <em>cannot</em> {@link PrimitiveSession#publish(PrimitiveEvent)} session
   *                events to this session.
   */
  protected void onOpen(PrimitiveSession session) {

  }

  /**
   * Called when a session is expired by the system.
   * <p>
   * This method is called when a client fails to keep its session alive with the cluster. If the leader hasn't heard
   * from a client for a configurable time interval, the leader will expire the session to free the related memory.
   * This method will always be called for a given session before {@link #onClose(PrimitiveSession)}, and {@link #onClose(PrimitiveSession)}
   * will always be called following this method.
   * <p>
   * State machines are free to {@link PrimitiveSession#publish(PrimitiveEvent)} session event messages to any session except
   * the one that expired. Session event messages sent to the session that expired will be lost since the session is closed once this
   * method call completes.
   *
   * @param session The session that was expired. State machines <em>cannot</em> {@link PrimitiveSession#publish(PrimitiveEvent)} session
   *                events to this session.
   */
  protected void onExpire(PrimitiveSession session) {

  }

  /**
   * Called when a session was closed by the client.
   * <p>
   * This method is called when a client explicitly closes a session.
   * <p>
   * State machines are free to {@link PrimitiveSession#publish(PrimitiveEvent)} session event messages to any session except
   * the one that was closed. Session event messages sent to the session that was closed will be lost since the session is closed once this
   * method call completes.
   *
   * @param session The session that was closed. State machines <em>cannot</em> {@link PrimitiveSession#publish(PrimitiveEvent)} session
   *                events to this session.
   */
  protected void onClose(PrimitiveSession session) {

  }

  /**
   * Session proxy.
   */
  private final class SessionProxy {
    private final PrimitiveSession session;
    private final C proxy;

    public SessionProxy(PrimitiveSession session, C proxy) {
      this.session = session;
      this.proxy = proxy;
    }

    /**
     * Publishes an event to the session.
     *
     * @param event the event to publish
     */
    void accept(Consumer<C> event) {
      event.accept(proxy);
    }
  }

  /**
   * Session proxy invocation handler.
   */
  private final class SessionProxyHandler implements InvocationHandler {
    private final PrimitiveSession session;
    private final Map<Method, EventType> events;

    private SessionProxyHandler(PrimitiveSession session) {
      this.session = session;
      this.events = clientInterface != null ? Events.getMethodMap(clientInterface) : Maps.newHashMap();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      EventType eventType = events.get(method);
      if (eventType == null) {
        throw new PrimitiveException.ServiceException("Cannot invoke unknown event type: " + method.getName());
      }
      session.publish(PrimitiveEvent.event(eventType, encode(args)));
      return null;
    }
  }
}
