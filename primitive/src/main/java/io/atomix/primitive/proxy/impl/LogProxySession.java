/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.proxy.impl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Defaults;
import com.google.common.collect.Maps;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.Events;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.log.LogRecord;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.Operations;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.impl.AbstractSession;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Log proxy session.
 */
public class LogProxySession<S> implements ProxySession<S> {
  private static final Serializer INTERNAL_SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
      .register(LogOperation.class)
      .register(DefaultOperationId.class)
      .register(OperationType.class)
      .register(SessionId.class)
      .build());

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final String name;
  private final PrimitiveType type;
  private final PrimitiveService service;
  private final ServiceConfig serviceConfig;
  private final ServiceContext context = new LogServiceContext();
  private final Serializer userSerializer;
  private final LogSession session;
  private final ServiceProxy<S> proxy;
  private volatile Object client;
  private volatile CompletableFuture<ProxySession<S>> connectFuture;
  private final AtomicLong operationIndex = new AtomicLong();
  private final Map<EventType, Method> eventMethods = Maps.newConcurrentMap();
  private final Map<Long, CompletableFuture> writeFutures = Maps.newConcurrentMap();
  private final Map<SessionId, Session> sessions = Maps.newConcurrentMap();
  private long currentIndex;
  private Session currentSession;
  private OperationType currentOperation;
  private long currentTimestamp;

  @SuppressWarnings("unchecked")
  public LogProxySession(String name, PrimitiveType type, Class<S> serviceType, ServiceConfig serviceConfig, Serializer serializer, LogSession session) {
    this.name = checkNotNull(name, "name cannot be null");
    this.type = checkNotNull(type, "type cannot be null");
    this.service = type.newService(serviceConfig);
    this.serviceConfig = serviceConfig;
    this.userSerializer = checkNotNull(serializer, "serializer cannot be null");
    this.session = checkNotNull(session, "session cannot be null");
    ServiceProxyHandler serviceProxyHandler = new ServiceProxyHandler(serviceType);
    S serviceProxy = (S) java.lang.reflect.Proxy.newProxyInstance(serviceType.getClassLoader(), new Class[]{serviceType}, serviceProxyHandler);
    proxy = new ServiceProxy<>(serviceProxy, serviceProxyHandler);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType type() {
    return type;
  }

  @Override
  public PartitionId partitionId() {
    return session.partitionId();
  }

  @Override
  public ThreadContext context() {
    return session.context();
  }

  @Override
  public PrimitiveState getState() {
    return session.getState();
  }

  @Override
  public void register(Object client) {
    this.client = client;
    Events.getEventMap(client.getClass()).forEach((eventType, method) -> eventMethods.put(eventType, method));
  }

  @Override
  public CompletableFuture<Void> accept(Consumer<S> operation) {
    if (session.getState() == PrimitiveState.CLOSED) {
      return Futures.exceptionalFuture(new PrimitiveException.ClosedSession());
    }
    return proxy.accept(operation);
  }

  @Override
  public <R> CompletableFuture<R> apply(Function<S, R> operation) {
    if (session.getState() == PrimitiveState.CLOSED) {
      return Futures.exceptionalFuture(new PrimitiveException.ClosedSession());
    }
    return proxy.apply(operation);
  }

  /**
   * Consumes a record from the log.
   *
   * @param record the record to consume
   */
  @SuppressWarnings("unchecked")
  private void consume(LogRecord record) {
    LogOperation operation = decodeInternal(record.value());

    Session session = sessions.get(operation.sessionId());
    if (session == null) {
      session = new LocalSession(operation.sessionId(), name(), type(), null, service.serializer());
      sessions.put(session.sessionId(), session);
      service.register(session);
    }

    currentIndex = record.index();
    currentSession = session;
    currentOperation = operation.operationId().type();
    currentTimestamp = record.timestamp();

    byte[] output = service.apply(new DefaultCommit<>(
        currentIndex,
        operation.operationId(),
        operation.operation(),
        currentSession,
        currentTimestamp));

    if (operation.sessionId().equals(this.session.sessionId())) {
      CompletableFuture future = writeFutures.remove(operation.operationIndex());
      if (future != null) {
        future.complete(decode(output));
      }
    }
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    session.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    session.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<ProxySession<S>> connect() {
    if (connectFuture == null) {
      synchronized (this) {
        if (connectFuture == null) {
          session.consumer().consume(1, this::consume);
          service.init(context);
          connectFuture = session.connect().thenApply(v -> this);
        }
      }
    }
    return connectFuture;
  }

  @Override
  public CompletableFuture<Void> close() {
    return session.close();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return close();
  }

  /**
   * Encodes the given object using the configured {@link #userSerializer}.
   *
   * @param object the object to encode
   * @param <T>    the object type
   * @return the encoded bytes
   */
  protected <T> byte[] encode(T object) {
    return object != null ? userSerializer.encode(object) : null;
  }

  /**
   * Decodes the given object using the configured {@link #userSerializer}.
   *
   * @param bytes the bytes to decode
   * @param <T>   the object type
   * @return the decoded object
   */
  protected <T> T decode(byte[] bytes) {
    return bytes != null ? userSerializer.decode(bytes) : null;
  }

  /**
   * Encodes an internal object.
   *
   * @param object the object to encode
   * @param <T> the object type
   * @return the encoded bytes
   */
  private <T> byte[] encodeInternal(T object) {
    return INTERNAL_SERIALIZER.encode(object);
  }

  /**
   * Decodes an internal object.
   *
   * @param bytes the bytes to decode
   * @param <T> the object type
   * @return the internal object
   */
  private <T> T decodeInternal(byte[] bytes) {
    return INTERNAL_SERIALIZER.decode(bytes);
  }

  /**
   * Log service context.
   */
  private class LogServiceContext implements ServiceContext {
    @Override
    public PrimitiveId serviceId() {
      return PrimitiveId.from(session.sessionId().id());
    }

    @Override
    public String serviceName() {
      return name;
    }

    @Override
    public PrimitiveType serviceType() {
      return type;
    }

    @Override
    public MemberId localMemberId() {
      return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C extends ServiceConfig> C serviceConfig() {
      return (C) serviceConfig;
    }

    @Override
    public long currentIndex() {
      return currentIndex;
    }

    @Override
    public Session currentSession() {
      return currentSession;
    }

    @Override
    public OperationType currentOperation() {
      return currentOperation;
    }

    @Override
    public LogicalClock logicalClock() {
      return new LogicalClock() {
        @Override
        public LogicalTimestamp getTime() {
          return LogicalTimestamp.of(currentIndex);
        }
      };
    }

    @Override
    public WallClock wallClock() {
      return new WallClock() {
        @Override
        public WallClockTimestamp getTime() {
          return WallClockTimestamp.from(currentTimestamp);
        }
      };
    }
  }

  /**
   * Local session.
   */
  private class LocalSession extends AbstractSession {
    LocalSession(
        SessionId sessionId,
        String primitiveName,
        PrimitiveType primitiveType,
        MemberId memberId,
        Serializer serializer) {
      super(sessionId, primitiveName, primitiveType, memberId, serializer);
    }

    @Override
    public State getState() {
      return State.OPEN;
    }

    @Override
    public void publish(PrimitiveEvent event) {
      if (sessionId().equals(session.sessionId())) {
        Method method = eventMethods.get(event.type());
        if (method != null) {
          try {
            method.invoke(client, (Object[]) decode(event.value()));
          } catch (IllegalAccessException | InvocationTargetException e) {
            log.warn("Failed to handle event", e);
          }
        }
      }
    }
  }

  /**
   * Service proxy container.
   */
  private class ServiceProxy<S> {
    private final S proxy;
    private final ServiceProxyHandler handler;

    ServiceProxy(S proxy, ServiceProxyHandler handler) {
      this.proxy = proxy;
      this.handler = handler;
    }

    /**
     * Invokes a void method on the underlying proxy.
     *
     * @param operation the operation to perform on the proxy
     * @return the resulting void future
     */
    CompletableFuture<Void> accept(Consumer<S> operation) {
      operation.accept(proxy);
      return handler.getResultFuture();
    }

    /**
     * Invokes a function on the underlying proxy.
     *
     * @param operation the operation to perform on the proxy
     * @param <T>       the operation return type
     * @return the future result
     */
    <T> CompletableFuture<T> apply(Function<S, T> operation) {
      operation.apply(proxy);
      return handler.getResultFuture();
    }
  }

  /**
   * Service proxy invocation handler.
   * <p>
   * The invocation handler
   */
  private class ServiceProxyHandler implements InvocationHandler {
    private final ThreadLocal<CompletableFuture> future = new ThreadLocal<>();
    private final Map<Method, OperationId> operations = new ConcurrentHashMap<>();

    private ServiceProxyHandler(Class<?> type) {
      this.operations.putAll(Operations.getMethodMap(type));
    }

    @Override
    public Object invoke(Object object, Method method, Object[] args) throws Throwable {
      OperationId operationId = operations.get(method);
      if (operationId != null) {
        CompletableFuture future = new CompletableFuture();
        this.future.set(future);
        long index = operationIndex.incrementAndGet();
        writeFutures.put(index, future);
        log.warn("ADD SESSION {} OPERATION {}: {}", session.sessionId(), operationIndex, operationId);
        connect().thenRun(() -> session.producer().append(encodeInternal(new LogOperation(session.sessionId(), index, operationId, encode(args)))));
      } else {
        throw new PrimitiveException("Unknown primitive operation: " + method.getName());
      }
      return Defaults.defaultValue(method.getReturnType());
    }

    /**
     * Returns the result future for the operation.
     *
     * @param <T> the future result type
     * @return the result future
     */
    @SuppressWarnings("unchecked")
    <T> CompletableFuture<T> getResultFuture() {
      return future.get();
    }
  }
}
